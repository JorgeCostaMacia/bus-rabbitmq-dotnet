using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors;

/// <summary>
/// Base of the event's error handler — the bus's own error lane, invoked when the event's
/// subscriber fails to decide the delivery's outcome (republish to retry or park to its error
/// queue). Framework-internal: a service tunes the lane through its subscriber registration
/// (<c>retryIntervals</c>, <c>retryExcludeExceptionTypes</c>) and the optional retry scheduler —
/// never by replacing this handler. Closes the <see cref="IHandler{TMessage, TContext}"/> contract
/// over <see cref="EventErrorContext{TEvent}"/>, so the error handler declares only its event type
/// and gets fully-typed access to the failed event, its envelope and the exception.
/// </summary>
/// <typeparam name="TEvent">The event type whose handling failures this handler manages.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber this error handler is paired with — ties it to its event and
/// subscriber, so each pairing is a distinct type resolvable on its own.</typeparam>
internal abstract class EventErrorHandlerBase<TEvent, TEventSubscriber> : IHandler<TEvent, EventErrorContext<TEvent>>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    /// <summary>How the handler left the delivery — set as it runs, read by the consumer afterwards. Defaults to
    /// <see cref="ErrorResult.Unhandled"/>.</summary>
    public ErrorResult Result { get; protected set; }

    /// <summary>Handles the terminal failure of an event delivery.</summary>
    /// <param name="context">The delivery's error context — the failed event, its envelope and the exception.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(
        EventErrorContext<TEvent> context,
        CancellationToken cancellationToken = default
    );
}
