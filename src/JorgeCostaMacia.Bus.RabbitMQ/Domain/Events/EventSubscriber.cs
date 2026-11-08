using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

/// <summary>
/// Ergonomic base for a RabbitMQ event subscriber: closes the <see cref="IHandler{TMessage, TContext}"/>
/// contract over <see cref="EventContext{TEvent}"/>, so a concrete subscriber declares only its event
/// type and gets fully-typed access to the message, transport and envelope.
/// <para>
/// Delivery is <b>at-least-once</b>: a subscriber may run more than once for the same event — an
/// un-acked delivery is redelivered, and a retry re-publishes it — and the retry path carries no
/// ordering guarantee. Subscribers must therefore be <b>idempotent</b> — deduplicate by the message's
/// id or reconcile by a version (its <c>AggregateOccurredAt</c> can serve as one), never assume a
/// once-only, in-order call.
/// </para>
/// </summary>
/// <typeparam name="TEvent">The event type this subscriber processes.</typeparam>
public abstract class EventSubscriber<TEvent> : IHandler<TEvent, EventContext<TEvent>>
    where TEvent : Event
{
    /// <summary>Handles the delivered event.</summary>
    /// <param name="context">The delivery's context — event, transport and read-only envelope.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(
        EventContext<TEvent> context,
        CancellationToken cancellationToken = default
    );
}
