using System.Collections.Immutable;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events;

/// <summary>
/// The default event error handler over the context the worker already built: a retryable failure
/// follows the interval ladder (a <c>00:00</c> interval re-publishes to the event's exchange
/// immediately, envelope cloned, <c>RetryCount</c> incremented and the retry re-targeted to this queue
/// only — the other subscriber queues already handled the original and filter the retry out; a
/// positive one is parked through the retry scheduler to be produced back at its time — or, with no
/// scheduler registered, parked to <c>{queue}.error</c> as terminal, since it cannot be delayed); a
/// terminal failure parks an <see cref="EventError{TEvent}"/> to the <c>{queue}.error</c> queue.
/// Reports through <c>Result</c>: a publish failure or a scheduler hiccup leaves it
/// <see cref="ErrorResult.Unhandled"/> (redelivers); an unexpected break reports <see cref="ErrorResult.Faulted"/>.
/// </summary>
/// <typeparam name="TEvent">The event type this handler manages the failures of.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber it is paired with.</typeparam>
internal sealed class EventErrorHandler<TEvent, TEventSubscriber> : EventErrorHandlerBase<TEvent, TEventSubscriber>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    private const string ErrorQueueSuffix = ".error";

    private readonly IProducer _producer;
    private readonly IRetryScheduler? _retryScheduler;
    private readonly ILogger _logger;
    private readonly string _exchange;
    private readonly string _queue;
    private readonly ImmutableList<TimeSpan> _retryIntervals;
    private readonly ImmutableList<Type> _retryExcludeExceptionTypes;

    /// <summary>Creates the handler over the outbound gate, the optional retry scheduler, the logger and the consumer's
    /// contract.</summary>
    /// <param name="producer">The outbound gate — retries re-publish and parks go through it.</param>
    /// <param name="retryScheduler">The scheduler parking delayed retries, or <see langword="null"/> when none is
    /// registered.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="exchange">The event's exchange — an immediate retry re-publishes to it.</param>
    /// <param name="queue">The consumer queue — the retry's targeting, where terminal failures park (its <c>.error</c>)
    /// and the failing queue stamped on them.</param>
    /// <param name="retryIntervals">The retry ladder — one delay per attempt, <c>00:00</c> re-publishes immediately
    /// (empty means no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exception types excluded from retry (inheritance-aware).</param>
    public EventErrorHandler(
        IProducer producer,
        IRetryScheduler? retryScheduler,
        ILogger logger,
        string exchange,
        string queue,
        ImmutableList<TimeSpan> retryIntervals,
        ImmutableList<Type> retryExcludeExceptionTypes
    )
    {
        _producer = producer;
        _retryScheduler = retryScheduler;
        _logger = logger;
        _exchange = exchange;
        _queue = queue;
        _retryIntervals = retryIntervals;
        _retryExcludeExceptionTypes = retryExcludeExceptionTypes;
    }

    /// <inheritdoc />
    public override async Task Handle(
        EventErrorContext<TEvent> context,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            if (!Retryable(context))
            {
                await ParkError(context, cancellationToken);

                using (BusLogger.DescriptionContext(BusLoggerDescriptions.ParkedToErrorQueue))
                {
                    _logger.LogError(context.Error, "Subscriber failed.");
                }

                Result = ErrorResult.Parked;

                return;
            }

            Result = _retryIntervals[context.RetryCount] > TimeSpan.Zero
                ? await Schedule(context, cancellationToken)
                : await Requeue(context, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            Result = ErrorResult.Unhandled;
        }
        catch (RabbitMQClientException produce)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.DeliveryNotAcked))
            {
                _logger.LogError(produce, "Producer failed.");
            }

            Result = ErrorResult.Unhandled;
        }
        catch (Exception broken)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.HandedToFaultHandler))
            {
                _logger.LogError(broken, "Error handler failed.");
            }

            Result = ErrorResult.Faulted;
        }
    }

    /// <summary>Whether the failed event is retried: a valid ladder position with entries left and an exception not
    /// excluded.</summary>
    private bool Retryable(EventErrorContext<TEvent> context)
        => context.RetryCount >= 0
            && context.RetryCount < _retryIntervals.Count
            && !_retryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(context.Error));

    /// <summary>Re-publishes the retry to the event's exchange, envelope cloned, retry count incremented and
    /// re-targeted to this queue only.</summary>
    private async Task<ErrorResult> Requeue(
        EventErrorContext<TEvent> context,
        CancellationToken cancellationToken
    )
    {
        await _producer.Produce(_exchange, string.Empty, Body(context), RetryHeaders(context), cancellationToken);

        BusLogger.LogRetry(_logger, context.Error, context.RetryCount + 1);

        return ErrorResult.Retried;
    }

    /// <summary>
    /// Parks a delayed retry through the scheduler — produced back to the exchange at its time. With no
    /// scheduler registered the failure cannot be delayed, so it parks to the error queue as terminal;
    /// a scheduler that fails reports <see cref="ErrorResult.Unhandled"/> — the delivery is nacked with requeue and
    /// redelivers.
    /// </summary>
    private async Task<ErrorResult> Schedule(
        EventErrorContext<TEvent> context,
        CancellationToken cancellationToken
    )
    {
        if (_retryScheduler is null)
        {
            await ParkError(context, cancellationToken);

            using (BusLogger.DescriptionContext(BusLoggerDescriptions.RetrySchedulerMissing))
            {
                _logger.LogError(context.Error, "Subscriber failed.");
            }

            return ErrorResult.Parked;
        }

        DateTime scheduledAt = DateTime.UtcNow.Add(_retryIntervals[context.RetryCount]);

        try
        {
            await _retryScheduler.Schedule(
                _exchange,
                _queue,
                Body(context),
                RetryHeaders(context),
                scheduledAt,
                cancellationToken);

            BusLogger.LogRetry(_logger, context.Error, context.RetryCount + 1, scheduledAt);

            return ErrorResult.Scheduled;
        }
        catch (OperationCanceledException)
        {
            return ErrorResult.Unhandled;
        }
        catch (Exception schedule)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.ScheduleFailed))
            {
                _logger.LogError(schedule, "Retry failed.");
            }

            return ErrorResult.Unhandled;
        }
    }

    /// <summary>Parks the subscriber failure to the error queue: an <see cref="EventError{TEvent}"/> built from the
    /// context, published via the default exchange to <c>{queue}.error</c>.</summary>
    private Task ParkError(
        EventErrorContext<TEvent> context,
        CancellationToken cancellationToken
    )
        => _producer.Park(
            _queue + ErrorQueueSuffix,
            JsonSerializer.SerializeToUtf8Bytes(
                EventError<TEvent>.Create(
                    context,
                    _queue),
                BusSerializer.Options),
            ErrorHeaders(context),
            cancellationToken);

    /// <summary>The retry's body — the typed event re-serialized.</summary>
    private static byte[] Body(EventErrorContext<TEvent> context)
        => JsonSerializer.SerializeToUtf8Bytes(context.Message, BusSerializer.Options);

    /// <summary>
    /// The retry's headers — the envelope cloned with <c>RetryCount</c> incremented and the retry
    /// re-targeted to this queue only (<c>AggregateConsumers</c>), so the fanout re-publish is skipped
    /// by every other subscriber instead of being reprocessed by all of them.
    /// </summary>
    private Dictionary<string, string> RetryHeaders(EventErrorContext<TEvent> context)
    {
        Dictionary<string, string> headers = context.Transport.CloneHeaders();

        TransportHeaders.Restamp(
            headers,
            TransportHeaders.RetryCount,
            TransportHeaders.ToHeader(context.RetryCount + 1));
        TransportHeaders.Restamp(headers, TransportHeaders.AggregateConsumers, TransportHeaders.ToHeader(_queue));

        return headers;
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing
    /// queue, the UTC time).</summary>
    private Dictionary<string, string> ErrorHeaders(EventErrorContext<TEvent> context)
    {
        Dictionary<string, string> headers = context.Transport.CloneHeaders();

        TransportHeaders.StampError(headers, context.Error, _queue);

        return headers;
    }
}
