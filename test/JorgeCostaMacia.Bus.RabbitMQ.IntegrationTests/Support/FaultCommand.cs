using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// A minimal command for the fault-parking test: it exists only to give the worker a queue and a
/// handler to deserialize into — the test delivers a malformed body that never deserializes to it, so
/// the delivery breaks before the handler and is parked to <c>{queue}.fault</c>.
/// </summary>
public sealed record FaultCommand : Command
{
    /// <summary>The payload (never reached — the malformed body fails to deserialize first).</summary>
    public string Payload { get; init; }

    /// <summary>Hydrating constructor used by the serializer on the consuming side.</summary>
    /// <param name="aggregateId">The command id.</param>
    /// <param name="aggregateCorrelationId">The correlation id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp.</param>
    /// <param name="aggregateConsumers">The target consumers.</param>
    /// <param name="payload">The payload.</param>
    [JsonConstructor]
    public FaultCommand(
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
    public FaultCommand(string payload)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
    {
        Payload = payload;
    }
}
