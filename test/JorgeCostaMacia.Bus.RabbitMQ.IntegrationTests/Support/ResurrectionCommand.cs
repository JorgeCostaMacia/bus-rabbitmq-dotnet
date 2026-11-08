using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// A command the resurrection test sends before and after a broker interruption, carrying a single
/// string payload so the test can tell the two deliveries apart.
/// </summary>
public sealed record ResurrectionCommand : Command
{
    /// <summary>The payload the handler records on receipt.</summary>
    public string Payload { get; init; }

    /// <summary>Hydrating constructor used by the serializer on the consuming side.</summary>
    /// <param name="aggregateId">The command id.</param>
    /// <param name="aggregateCorrelationId">The correlation id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp.</param>
    /// <param name="aggregateConsumers">The target consumers.</param>
    /// <param name="payload">The payload.</param>
    [JsonConstructor]
    public ResurrectionCommand(
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
    public ResurrectionCommand(string payload)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
    {
        Payload = payload;
    }
}
