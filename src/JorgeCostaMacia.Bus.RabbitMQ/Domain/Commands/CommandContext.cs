using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

/// <summary>
/// The RabbitMQ command context a handler receives — composes the envelope facets over
/// <see cref="Transport"/> (all but the filtering one: commands are point-to-point and never
/// filtered), carrying only the command and the transport: every envelope property reads straight
/// from the transport's headers on access, nothing duplicated in memory. Built by the consumer, which
/// deserializes the message once per delivery; the <b>outbound</b> envelope is computed by the bus
/// when producing, not here.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public record CommandContext<TCommand> :
    IMessageContext<TCommand>,
    ITransportContext<Transport>,
    ITracedContext,
    IAggregateTracedContext,
    IConversationContext,
    IResilientContext,
    IHostContext
    where TCommand : Command
{
    /// <summary>The delivered command.</summary>
    public TCommand Message { get; init; }

    /// <summary>The transport this command arrived on (RabbitMQ headers / delivery tag / …).</summary>
    public Transport Transport { get; init; }

    /// <summary>Unique id of this message, assigned by the messaging layer.</summary>
    public Guid MessageId => Transport.GetHeaderGuid(TransportHeaders.MessageId);

    /// <summary>Logical type name of the message.</summary>
    public string MessageType => Transport.GetHeaderString(TransportHeaders.MessageType);

    /// <summary>Primary destination address (exchange).</summary>
    public string MessageDestinationAddress => Transport.GetHeaderString(TransportHeaders.MessageDestinationAddress);

    /// <summary>Primary origin address (exchange) the message came from, when known.</summary>
    public string? MessageOriginAddress => Transport.GetHeaderStringOrDefault(TransportHeaders.MessageOriginAddress);

    /// <summary>UTC time when the message was created/sent.</summary>
    public DateTime MessageOccurredAt => Transport.GetHeaderDateTime(TransportHeaders.MessageOccurredAt);

    /// <summary>Conversation trace id, shared by the whole chain; equals the first message's id.</summary>
    public Guid ConversationId => Transport.GetHeaderGuid(TransportHeaders.ConversationId);

    /// <summary>Address where the conversation originated — the first message's destination.</summary>
    public string ConversationAddress => Transport.GetHeaderString(TransportHeaders.ConversationAddress);

    /// <summary>UTC time when the conversation began.</summary>
    public DateTime ConversationOccurredAt => Transport.GetHeaderDateTime(TransportHeaders.ConversationOccurredAt);

    /// <summary>Unique id of the inbound message (domain trace).</summary>
    public Guid AggregateId => Transport.GetHeaderGuid(TransportHeaders.AggregateId);

    /// <summary>Domain correlation id, propagated to messages sent from this handler.</summary>
    public Guid AggregateCorrelationId => Transport.GetHeaderGuid(TransportHeaders.AggregateCorrelationId);

    /// <summary>UTC event-time of the inbound message.</summary>
    public DateTime AggregateOccurredAt => Transport.GetHeaderDateTime(TransportHeaders.AggregateOccurredAt);

    /// <summary>Number of times this message has been retried (immediate or scheduled).</summary>
    public int RetryCount => Transport.GetHeaderInt(TransportHeaders.RetryCount);

    /// <summary>The machine (host) name that produced the message.</summary>
    public string HostMachineName => Transport.GetHeaderString(TransportHeaders.HostMachineName);

    /// <summary>The entry assembly's simple name of the producing host.</summary>
    public string HostAssembly => Transport.GetHeaderString(TransportHeaders.HostAssembly);

    /// <summary>The entry assembly's version of the producing host.</summary>
    public string HostAssemblyVersion => Transport.GetHeaderString(TransportHeaders.HostAssemblyVersion);

    /// <summary>The .NET runtime version of the producing host.</summary>
    public string HostFrameworkVersion => Transport.GetHeaderString(TransportHeaders.HostFrameworkVersion);

    /// <summary>The bus library version of the producing host.</summary>
    public string HostBusVersion => Transport.GetHeaderString(TransportHeaders.HostBusVersion);

    /// <summary>The operating system version of the producing host.</summary>
    public string HostOperatingSystemVersion => Transport.GetHeaderString(TransportHeaders.HostOperatingSystemVersion);

    /// <summary>Builds the context over the delivered command and its transport.</summary>
    /// <param name="message">The command payload.</param>
    /// <param name="transport">The RabbitMQ transport for this delivery.</param>
    public CommandContext(
        TCommand message,
        Transport transport
    )
    {
        Message = message;
        Transport = transport;
    }
}
