using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// A minimal event for the fanout test: an <see cref="Event"/> carrying a single string payload. The
/// exact-typed <see cref="JsonConstructor"/> hydrates it on each subscriber's side; the convenience
/// constructor generates the traceability metadata when publishing.
/// </summary>
public sealed record FanoutEvent : Event
{
    /// <summary>The payload broadcast to every subscriber and asserted on receipt.</summary>
    public string Payload { get; init; }

    /// <summary>Hydrating constructor used by the serializer on each subscriber's side.</summary>
    /// <param name="aggregateId">The event id.</param>
    /// <param name="aggregateCorrelationId">The correlation id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp.</param>
    /// <param name="aggregateConsumers">The target consumers.</param>
    /// <param name="payload">The payload.</param>
    [JsonConstructor]
    public FanoutEvent(
        Guid aggregateId,
        Guid aggregateCorrelationId,
        DateTime aggregateOccurredAt,
        ImmutableList<string> aggregateConsumers,
        string payload
    ) : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
    {
        Payload = payload;
    }

    /// <summary>Convenience constructor used to publish the event, defaulting the metadata.</summary>
    /// <param name="payload">The payload.</param>
    public FanoutEvent(string payload)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
    {
        Payload = payload;
    }
}
