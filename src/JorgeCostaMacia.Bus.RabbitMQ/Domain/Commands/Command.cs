using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

/// <summary>
/// Base implementation for commands on the RabbitMQ bus: an immutable <see langword="record"/>
/// carrying traceability metadata (id / correlation / UTC timestamp) and optional target consumers,
/// defaulting the id to a time-ordered UUIDv7. Concrete commands forward to it with
/// <c>: base(...)</c>. Implements the transport-agnostic message contracts
/// (<see cref="ITracedMessage"/> / <see cref="IFilteredMessage"/>).
/// </summary>
public abstract record Command : ITracedMessage, IFilteredMessage
{
    /// <summary>Unique identifier of this command instance.</summary>
    public Guid AggregateId { get; init; }

    /// <summary>Correlation identifier shared by every message in the same workflow.</summary>
    public Guid AggregateCorrelationId { get; init; }

    /// <summary>UTC timestamp of when the command was issued.</summary>
    public DateTime AggregateOccurredAt { get; init; }

    /// <summary>
    /// The consumers the events generated from this command will target; empty means no filtering.
    /// Commands themselves are point-to-point and never filtered — this travels as data for the
    /// handler to stamp on the events it publishes.
    /// </summary>
    public ImmutableList<string> AggregateConsumers { get; init; }

    /// <summary>
    /// Initializes the traceability metadata, generating defaults when a value is not supplied: the
    /// id via a time-ordered UUIDv7, the correlation id defaulting to the id, and the timestamp to
    /// <see cref="DateTime.UtcNow"/>.
    /// </summary>
    /// <param name="aggregateId">The command id, or <see langword="null"/> to generate one.</param>
    /// <param name="aggregateCorrelationId">The correlation id, or <see langword="null"/> to default to the id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp, or <see langword="null"/> for now.</param>
    /// <param name="aggregateConsumers">The target consumers, or <see langword="null"/> for none.</param>
    protected Command(
        Guid? aggregateId,
        Guid? aggregateCorrelationId,
        DateTime? aggregateOccurredAt,
        IEnumerable<string>? aggregateConsumers
    )
    {
        AggregateId = aggregateId ?? Guid.CreateVersion7();
        AggregateCorrelationId = aggregateCorrelationId ?? AggregateId;
        AggregateOccurredAt = aggregateOccurredAt ?? DateTime.UtcNow;
        AggregateConsumers = aggregateConsumers?.ToImmutableList() ?? ImmutableList<string>.Empty;
    }
}
