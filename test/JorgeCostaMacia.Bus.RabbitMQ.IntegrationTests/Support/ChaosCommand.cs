using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// A generic command for the recovery / idempotency chaos tests: carries a stable <see cref="Payload"/>
/// used as the record's identity, so a handler can deduplicate redeliveries (the at-least-once
/// duplicates a broker outage or a redelivery produces) and the test can assert every distinct record
/// was handled at least once with none lost.
/// </summary>
public sealed record ChaosCommand : Command
{
    /// <summary>The record's stable identity — deduplicated on the consuming side to tell a unique record from a
    /// redelivery.</summary>
    public string Payload { get; init; }

    /// <summary>Hydrating constructor used by the serializer on the consuming side.</summary>
    /// <param name="aggregateId">The command id.</param>
    /// <param name="aggregateCorrelationId">The correlation id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp.</param>
    /// <param name="aggregateConsumers">The target consumers.</param>
    /// <param name="payload">The payload.</param>
    [JsonConstructor]
    public ChaosCommand(
        Guid aggregateId,
        Guid aggregateCorrelationId,
        DateTime aggregateOccurredAt,
        ImmutableList<string> aggregateConsumers,
        string payload
    ) : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
    {
        Payload = payload;
    }

    /// <summary>Convenience constructor used to send the command, defaulting the metadata.</summary>
    /// <param name="payload">The payload.</param>
    public ChaosCommand(string payload)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
    {
        Payload = payload;
    }
}
