using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events;

/// <summary>
/// The default event fault handler — parks an <see cref="EventFault"/> to the event queue's
/// <c>.fault</c> over the raw, never-deserialized body, the envelope cloned in the headers with the
/// failure stamped on top. Runs for a malformed event delivery, and as the relay when the event error
/// handler could not cope. Reports through <c>Result</c>: <see cref="FaultResult.Parked"/> acks,
/// <see cref="FaultResult.Unhandled"/> leaves it to redeliver. Never throws for control flow.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber it is paired with.</typeparam>
internal sealed class EventFaultHandler<TEvent, TEventSubscriber> : EventFaultHandlerBase<TEvent, TEventSubscriber>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    private const string FaultQueueSuffix = ".fault";

    private readonly IProducer _producer;
    private readonly ILogger _logger;
    private readonly string _queue;

    /// <summary>Creates the handler over the outbound producer, the logger and the consumer's queue.</summary>
    /// <param name="producer">The outbound gate — the fault parking goes through it.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="queue">The consumer queue — faults park to its <c>.fault</c>, and it is stamped on the parked fault
    /// as the failing queue.</param>
    public EventFaultHandler(
        IProducer producer,
        ILogger logger,
        string queue
    )
    {
        _producer = producer;
        _logger = logger;
        _queue = queue;
    }

    /// <inheritdoc />
    public override async Task Handle(
        EventFaultContext context,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            EventFault fault = EventFault.Create(
                context,
                _queue);

            await _producer.Park(
                _queue + FaultQueueSuffix,
                JsonSerializer.SerializeToUtf8Bytes(fault, BusSerializer.Options),
                FaultHeaders(context),
                cancellationToken);

            using (BusLogger.DescriptionContext(BusLoggerDescriptions.ParkedToFaultQueue))
            {
                _logger.LogError(context.Error, "Delivery faulted.");
            }

            Result = FaultResult.Parked;
        }
        catch (OperationCanceledException)
        {
            Result = FaultResult.Unhandled;
        }
        catch (Exception park)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.DeliveryNotAcked))
            {
                _logger.LogError(park, "Parking failed.");
            }

            Result = FaultResult.Unhandled;
        }
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing
    /// queue, the UTC time) — filterable and reinjectable header-side.</summary>
    private Dictionary<string, string> FaultHeaders(EventFaultContext context)
    {
        Dictionary<string, string> headers = context.Transport.CloneHeaders();

        TransportHeaders.StampError(headers, context.Error, _queue);

        return headers;
    }
}
