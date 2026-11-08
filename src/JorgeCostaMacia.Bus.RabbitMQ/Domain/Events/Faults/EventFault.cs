using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults;

/// <summary>
/// The body parked to an event's fault queue — the broken delivery's transport details together with
/// every error detail and the raw body as text, without ever deserializing the message. The original
/// envelope still travels cloned in the parked record's headers, with the failure stamped on top.
/// </summary>
public sealed record EventFault : IErrorMessage
{
    /// <summary>The failure that broke the delivery, modeled with its whole inner-exception chain.</summary>
    public ErrorInfo Error { get; init; }

    /// <summary>UTC time the failure was parked.</summary>
    public DateTime ErrorOccurredAt { get; init; }

    /// <summary>The queue whose delivery broke.</summary>
    public string Queue { get; init; }

    /// <summary>The host (machine) whose consumer broke — identifies the instance/replica for triage.</summary>
    public string MachineName { get; init; }

    /// <summary>The exchange the delivery broke on.</summary>
    public string Exchange { get; init; }

    /// <summary>The routing key the broken message was published with.</summary>
    public string RoutingKey { get; init; }

    /// <summary>The broker-assigned delivery tag of the broken message.</summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>Whether the broken message had been redelivered.</summary>
    public bool Redelivered { get; init; }

    /// <summary>The delivery's headers as best-effort text — the envelope's binary GUID headers render as
    /// GUIDs.</summary>
    public ImmutableList<KeyValuePair<string, string>> Headers { get; init; }

    /// <summary>The delivered raw body as text — never deserialized.</summary>
    public string Message { get; init; }

    /// <summary>Creates the parked body over the failure's details and the broken delivery.</summary>
    public EventFault(
        ErrorInfo error,
        DateTime errorOccurredAt,
        string queue,
        string machineName,
        string exchange,
        string routingKey,
        ulong deliveryTag,
        bool redelivered,
        ImmutableList<KeyValuePair<string, string>> headers,
        string message
    )
    {
        Error = error;
        ErrorOccurredAt = errorOccurredAt;
        Queue = queue;
        MachineName = machineName;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        Headers = headers;
        Message = message;
    }

    /// <summary>Builds the parked body from the fault context the consumer already built — stamping the failing
    /// queue.</summary>
    /// <param name="context">The broken delivery's fault context.</param>
    /// <param name="queue">The queue whose delivery broke.</param>
    /// <returns>The body parked to the fault queue.</returns>
    internal static EventFault Create(
        EventFaultContext context,
        string queue
    )
        => new EventFault(
            ErrorInfo.Create(context.Error),
            DateTime.UtcNow,
            queue,
            Environment.MachineName,
            context.Transport.Exchange,
            context.Transport.RoutingKey,
            context.Transport.DeliveryTag,
            context.Transport.Redelivered,
            context.Transport.DecodeHeaders(),
            context.Message);
}
