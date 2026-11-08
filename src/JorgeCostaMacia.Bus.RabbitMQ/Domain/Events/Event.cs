using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Messages;
using JorgeCostaMacia.DDD.DomainEvent.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

/// <summary>
/// Base implementation for events on the RabbitMQ bus: an immutable <see langword="record"/> carrying
/// traceability metadata (id / correlation / UTC timestamp) and optional target consumers,
/// defaulting the id to a time-ordered UUIDv7. Concrete events forward to it with
/// <c>: base(...)</c>. Implements the transport-agnostic message contracts
/// (<see cref="ITracedMessage"/> / <see cref="IFilteredMessage"/>) and <see cref="IDomainEvent"/>,
/// so it fits an aggregate's event list.
/// </summary>
public abstract record Event : IDomainEvent, ITracedMessage, IFilteredMessage
{
    /// <summary>Unique identifier of this event instance.</summary>
    public Guid AggregateId { get; init; }

    /// <summary>Correlation identifier shared by every message in the same workflow.</summary>
    public Guid AggregateCorrelationId { get; init; }

    /// <summary>UTC timestamp of when the event occurred.</summary>
    public DateTime AggregateOccurredAt { get; init; }

    /// <summary>The consumers this event targets (e.g. consumer group ids); empty means no filtering.</summary>
    public ImmutableList<string> AggregateConsumers { get; init; }

    /// <summary>
    /// Initializes the traceability metadata, generating defaults when a value is not supplied: the
    /// id via a time-ordered UUIDv7, the correlation id defaulting to the id, and the timestamp to
    /// <see cref="DateTime.UtcNow"/>.
    /// </summary>
    /// <param name="aggregateId">The event id, or <see langword="null"/> to generate one.</param>
    /// <param name="aggregateCorrelationId">The correlation id, or <see langword="null"/> to default to the id.</param>
    /// <param name="aggregateOccurredAt">The UTC timestamp, or <see langword="null"/> for now.</param>
    /// <param name="aggregateConsumers">The target consumers, or <see langword="null"/> for none.</param>
    protected Event(
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
