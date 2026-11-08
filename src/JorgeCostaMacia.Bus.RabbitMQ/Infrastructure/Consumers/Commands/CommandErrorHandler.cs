using System.Collections.Immutable;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Commands;

/// <summary>
/// The default command error handler over the context the worker already built: a retryable failure
/// follows the interval ladder (a <c>00:00</c> interval re-publishes to the command's exchange
/// immediately, envelope cloned and <c>RetryCount</c> incremented; a positive one is parked through
/// the retry scheduler to be produced back at its time — or, with no scheduler registered, parked to
/// <c>{queue}.error</c> as terminal, since it cannot be delayed); a terminal failure parks a
/// <see cref="CommandError{TCommand}"/> to the <c>{queue}.error</c> queue. Reports through
/// <c>Result</c>: a publish failure or a scheduler hiccup leaves it <see cref="ErrorResult.Unhandled"/>
/// (redelivers); an unexpected break reports <see cref="ErrorResult.Faulted"/>.
/// </summary>
/// <typeparam name="TCommand">The command type this handler manages the failures of.</typeparam>
/// <typeparam name="TCommandHandler">The command handler it is paired with.</typeparam>
internal sealed class CommandErrorHandler<TCommand, TCommandHandler> :
    CommandErrorHandlerBase<TCommand, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
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
    /// <param name="exchange">The command's exchange — an immediate retry re-publishes to it.</param>
    /// <param name="queue">The consumer queue — terminal failures park to its <c>.error</c>, and it is stamped on the
    /// parked error as the failing queue.</param>
    /// <param name="retryIntervals">The retry ladder — one delay per attempt, <c>00:00</c> re-publishes immediately
    /// (empty means no retries).</param>
    /// <param name="retryExcludeExceptionTypes">Exception types excluded from retry (inheritance-aware).</param>
    public CommandErrorHandler(
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
        CommandErrorContext<TCommand> context,
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
                    _logger.LogError(context.Error, "Handler failed.");
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

    /// <summary>Whether the failed command is retried: a valid ladder position with entries left and an exception not
    /// excluded.</summary>
    private bool Retryable(CommandErrorContext<TCommand> context)
        => context.RetryCount >= 0
            && context.RetryCount < _retryIntervals.Count
            && !_retryExcludeExceptionTypes.Any(type => type.IsInstanceOfType(context.Error));

    /// <summary>Re-publishes the retry to the command's exchange, envelope cloned and retry count
    /// incremented.</summary>
    private async Task<ErrorResult> Requeue(
        CommandErrorContext<TCommand> context,
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
        CommandErrorContext<TCommand> context,
        CancellationToken cancellationToken
    )
    {
        if (_retryScheduler is null)
        {
            await ParkError(context, cancellationToken);

            using (BusLogger.DescriptionContext(BusLoggerDescriptions.RetrySchedulerMissing))
            {
                _logger.LogError(context.Error, "Handler failed.");
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

    /// <summary>Parks the handler failure to the error queue: a <see cref="CommandError{TCommand}"/> built from the
    /// context, published via the default exchange to <c>{queue}.error</c>.</summary>
    private Task ParkError(
        CommandErrorContext<TCommand> context,
        CancellationToken cancellationToken
    )
        => _producer.Park(
            _queue + ErrorQueueSuffix,
            JsonSerializer.SerializeToUtf8Bytes(
                CommandError<TCommand>.Create(
                    context,
                    _queue),
                BusSerializer.Options),
            ErrorHeaders(context),
            cancellationToken);

    /// <summary>The retry's body — the typed command re-serialized.</summary>
    private static byte[] Body(CommandErrorContext<TCommand> context)
        => JsonSerializer.SerializeToUtf8Bytes(context.Message, BusSerializer.Options);

    /// <summary>The retry's headers — the envelope cloned with <c>RetryCount</c> incremented.</summary>
    private static Dictionary<string, string> RetryHeaders(CommandErrorContext<TCommand> context)
    {
        Dictionary<string, string> headers = context.Transport.CloneHeaders();

        TransportHeaders.Restamp(
            headers,
            TransportHeaders.RetryCount,
            TransportHeaders.ToHeader(context.RetryCount + 1));

        return headers;
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing
    /// queue, the UTC time).</summary>
    private Dictionary<string, string> ErrorHeaders(CommandErrorContext<TCommand> context)
    {
        Dictionary<string, string> headers = context.Transport.CloneHeaders();

        TransportHeaders.StampError(headers, context.Error, _queue);

        return headers;
    }
}
