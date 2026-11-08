using System.Text;
using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults;

/// <summary>
/// The context an event's fault handler receives when a delivery breaks before its subscriber runs
/// (undeserializable body, unreadable envelope) — composes only the facets guaranteed however broken
/// the message is: <see cref="IMessageContext{TMessage}"/> over <see cref="string"/> (the raw body,
/// never deserialized), the <see cref="Transport"/>, and the <see cref="IErrorContext{TError}"/>
/// facet carrying the failure. In-process only, never serialized.
/// </summary>
public sealed record EventFaultContext :
    IMessageContext<string>,
    ITransportContext<Transport>,
    IErrorContext<Exception>
{
    /// <summary>The delivered raw body as text — never deserialized.</summary>
    public string Message { get; init; }

    /// <summary>The transport the broken message arrived on (RabbitMQ headers / delivery tag / …).</summary>
    public Transport Transport { get; init; }

    /// <summary>The failure that broke the delivery.</summary>
    public Exception Error { get; init; }

    /// <summary>Builds the context over the delivery's raw body, its transport and its failure.</summary>
    /// <param name="message">The delivered raw body as text.</param>
    /// <param name="transport">The broken delivery's transport.</param>
    /// <param name="exception">The failure that broke the delivery.</param>
    public EventFaultContext(
        string message,
        Transport transport,
        Exception exception
    )
    {
        Message = message;
        Transport = transport;
        Error = exception;
    }

    /// <summary>Builds the context for a broken delivery, decoding the raw body as UTF-8 text.</summary>
    /// <param name="body">The delivered message's raw body.</param>
    /// <param name="transport">The broken delivery's transport.</param>
    /// <param name="exception">The failure that broke the delivery.</param>
    /// <returns>The context handed to the fault handler.</returns>
    internal static EventFaultContext Create(
        ReadOnlyMemory<byte> body,
        Transport transport,
        Exception exception
    )
        => new EventFaultContext(
            Encoding.UTF8.GetString(body.Span),
            transport,
            exception);
}
