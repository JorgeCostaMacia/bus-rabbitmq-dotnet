using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// A minimal command for the immediate-requeue test: its handler fails the first delivery and succeeds
/// on the redelivery re-published by a <c>00:00</c> retry step — exercising the immediate-requeue path
/// (distinct from the Quartz scheduled retry).
/// </summary>
public sealed record RequeueCommand : Command
{
    /// <summary>The payload carried across the failing delivery and its immediate redelivery.</summary>
    public string Payload { get; init; }

    /// <summary>Hydrating constructor used by the serializer on the consuming side.</summary>
    /// <param name="aggregateId">The command id.</param>
    /// <param name="aggregateCorrelationId">The correlation id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp.</param>
    /// <param name="aggregateConsumers">The target consumers.</param>
    /// <param name="payload">The payload.</param>
    [JsonConstructor]
    public RequeueCommand(
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
    public RequeueCommand(string payload)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
    {
        Payload = payload;
    }
}
