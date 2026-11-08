using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors;

/// <summary>
/// The event context an error handler receives when a delivery fails terminally — the delivery's
/// <see cref="EventContext{TEvent}"/> itself extended with the <see cref="IErrorContext{TError}"/>
/// facet carrying the failure. In-process only, never serialized.
/// </summary>
/// <typeparam name="TEvent">The event type.</typeparam>
public sealed record EventErrorContext<TEvent> :
    EventContext<TEvent>,
    IErrorContext<Exception>
    where TEvent : Event
{
    /// <summary>The failure that exhausted the delivery.</summary>
    public Exception Error { get; init; }

    /// <summary>Builds the error context over the failed event, its transport and its failure.</summary>
    /// <param name="message">The event payload.</param>
    /// <param name="transport">The RabbitMQ transport for this delivery.</param>
    /// <param name="error">The failure that exhausted the delivery.</param>
    public EventErrorContext(
        TEvent message,
        Transport transport,
        Exception error
    ) : base(message, transport)
    {
        Error = error;
    }
}
