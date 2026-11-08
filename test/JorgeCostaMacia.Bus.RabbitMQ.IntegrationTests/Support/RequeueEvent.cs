using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// A minimal event for the retry re-targeting test: published once with empty <c>AggregateConsumers</c>
/// so both subscriber queues process the original. One subscriber fails once and its immediate retry is
/// re-targeted (via the <c>AggregateConsumers</c> header) to that subscriber's group only — the other
/// subscriber, which already handled the original, filters the retry out.
/// </summary>
public sealed record RequeueEvent : Event
{
    /// <summary>The payload carried across the failing delivery and its re-targeted redelivery.</summary>
    public string Payload { get; init; }

    /// <summary>Hydrating constructor used by the serializer on each subscriber's side.</summary>
    /// <param name="aggregateId">The event id.</param>
    /// <param name="aggregateCorrelationId">The correlation id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp.</param>
    /// <param name="aggregateConsumers">The target consumers.</param>
    /// <param name="payload">The payload.</param>
    [JsonConstructor]
    public RequeueEvent(
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
    public RequeueEvent(string payload)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
    {
        Payload = payload;
    }
}
