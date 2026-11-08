using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// A minimal command for the roundtrip test: a <see cref="Command"/> carrying a single string
/// payload. The exact-typed <see cref="JsonConstructor"/> hydrates it on the consuming side; the
/// convenience constructor generates the traceability metadata when sending.
/// </summary>
public sealed record IntegrationCommand : Command
{
    /// <summary>The payload carried end to end and asserted on receipt.</summary>
    public string Payload { get; init; }

    /// <summary>Hydrating constructor used by the serializer on the consuming side.</summary>
    /// <param name="aggregateId">The command id.</param>
    /// <param name="aggregateCorrelationId">The correlation id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp.</param>
    /// <param name="aggregateConsumers">The target consumers.</param>
    /// <param name="payload">The payload.</param>
    [JsonConstructor]
    public IntegrationCommand(
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
    public IntegrationCommand(string payload)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
    {
        Payload = payload;
    }
}
