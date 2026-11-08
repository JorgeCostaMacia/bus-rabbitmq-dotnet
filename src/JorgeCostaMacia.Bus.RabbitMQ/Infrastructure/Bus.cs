using System.Text.Json;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.Domain.Messages;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using IBus = JorgeCostaMacia.Bus.RabbitMQ.Domain.IBus;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The RabbitMQ bus — the domain-facing facade over the outbound gate: it owns the routing map and,
/// for every <c>Send</c>/<c>Publish</c>, resolves the exchange, prepares the envelope (fresh, or
/// continued from an inbound transport) and publishes through the <see cref="IProducer"/> gate. Both
/// send and publish are one <c>basic.publish</c> to the message's exchange with an empty routing key —
/// command vs event is topology (a command exchange binds one queue, an event exchange fans out), not
/// a different publish. The gate is the single point every outbound byte goes through.
/// </summary>
internal sealed class Bus : IBus
{
    private const string RoutingKey = "";

    private readonly IProducer _producer;
    private readonly IReadOnlyDictionary<Type, string> _messages;

    /// <summary>Creates the bus over the outbound gate and the type → exchange routing map.</summary>
    /// <param name="producer">The outbound gate every send publishes through.</param>
    /// <param name="messages">The type → exchange routing map (commands and events).</param>
    public Bus(
        IProducer producer,
        IReadOnlyDictionary<Type, string> messages
    )
    {
        _producer = producer;
        _messages = messages;
    }

    /// <inheritdoc />
    public Task Send<T>(
        T message,
        CancellationToken cancellationToken = default
    )
        where T : Command
        => Produce(message, cancellationToken);

    /// <inheritdoc />
    public Task Send<T>(
        T message,
        ITransport transport,
        CancellationToken cancellationToken = default
    )
        where T : Command
        => Produce(message, transport, cancellationToken);

    /// <inheritdoc />
    public Task Publish<T>(
        T message,
        CancellationToken cancellationToken = default
    )
        where T : Event
        => Produce(message, cancellationToken);

    /// <inheritdoc />
    public Task Publish<T>(
        T message,
        ITransport transport,
        CancellationToken cancellationToken = default
    )
        where T : Event
        => Produce(message, transport, cancellationToken);

    /// <inheritdoc />
    public Task Send<T>(
        IEnumerable<T> messages,
        CancellationToken cancellationToken = default
    )
        where T : Command
        => Produce(messages, cancellationToken);

    /// <inheritdoc />
    public Task Send<T>(
        IEnumerable<T> messages,
        ITransport transport,
        CancellationToken cancellationToken = default
    )
        where T : Command
        => Produce(messages, transport, cancellationToken);

    /// <inheritdoc />
    public Task Publish<T>(
        IEnumerable<T> messages,
        CancellationToken cancellationToken = default
    )
        where T : Event
        => Produce(messages, cancellationToken);

    /// <inheritdoc />
    public Task Publish<T>(
        IEnumerable<T> messages,
        ITransport transport,
        CancellationToken cancellationToken = default
    )
        where T : Event
        => Produce(messages, transport, cancellationToken);

    /// <summary>Publishes a message with a fresh envelope to its exchange.</summary>
    private Task Produce<TMessage>(
        TMessage message,
        CancellationToken cancellationToken
    )
        where TMessage : ITracedMessage, IFilteredMessage
    {
        string exchange = Exchange(message);

        return _producer.Produce(
            exchange,
            RoutingKey,
            JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), BusSerializer.Options),
            Prepare(exchange, message),
            cancellationToken);
    }

    /// <summary>Publishes a message continuing an inbound flow to its exchange.</summary>
    private Task Produce<TMessage>(
        TMessage message,
        ITransport transport,
        CancellationToken cancellationToken
    )
        where TMessage : ITracedMessage, IFilteredMessage
    {
        string exchange = Exchange(message);

        return _producer.Produce(
            exchange,
            RoutingKey,
            JsonSerializer.SerializeToUtf8Bytes(message, message.GetType(), BusSerializer.Options),
            Prepare(exchange, message, transport),
            cancellationToken);
    }

    /// <summary>Publishes a batch, each with a fresh envelope, to their exchanges — concurrently: the destination
    /// channels pipeline the publishes and track each confirmation; awaited together, the first failure throws while
    /// the rest still publish.</summary>
    private Task Produce<TMessage>(
        IEnumerable<TMessage> messages,
        CancellationToken cancellationToken
    )
        where TMessage : ITracedMessage, IFilteredMessage
        => Task.WhenAll(messages.Select(message => Produce(message, cancellationToken)));

    /// <summary>Publishes a batch continuing an inbound flow to their exchanges — concurrently: the destination
    /// channels pipeline the publishes and track each confirmation; awaited together, the first failure throws while
    /// the rest still publish.</summary>
    private Task Produce<TMessage>(
        IEnumerable<TMessage> messages,
        ITransport transport,
        CancellationToken cancellationToken
    )
        where TMessage : ITracedMessage, IFilteredMessage
        => Task.WhenAll(messages.Select(message => Produce(message, transport, cancellationToken)));

    /// <summary>Resolves the exchange a message is routed to from the routing map.</summary>
    private string Exchange<TMessage>(TMessage message)
    {
        Type type = message!.GetType();

        if (!_messages.TryGetValue(type, out string? exchange))
        {
            throw new InvalidOperationException(
                $"'{type.FullName}' is not mapped to an exchange; map it with AddCommand/AddEvent first.");
        }

        return exchange;
    }

    /// <summary>
    /// Builds the envelope headers with a fresh conversation: a new message id, the conversation begins
    /// here (its id/address/time mirror this message), no origin, the retry counter at zero. The domain
    /// trace comes from the message itself.
    /// </summary>
    private static Dictionary<string, string> Prepare<TMessage>(
        string exchange,
        TMessage message
    )
        where TMessage : ITracedMessage, IFilteredMessage
    {
        Guid messageId = Guid.CreateVersion7();
        string occurredAt = DateTime.UtcNow.ToString("O");
        Type type = message.GetType();

        return new Dictionary<string, string>
        {
            [TransportHeaders.MessageId] = TransportHeaders.ToHeader(messageId),
            [TransportHeaders.MessageType] = TransportHeaders.ToHeader(type.FullName ?? type.Name),
            [TransportHeaders.MessageDestinationAddress] = TransportHeaders.ToHeader(exchange),
            [TransportHeaders.MessageOccurredAt] = TransportHeaders.ToHeader(occurredAt),
            [TransportHeaders.ConversationId] = TransportHeaders.ToHeader(messageId),
            [TransportHeaders.ConversationAddress] = TransportHeaders.ToHeader(exchange),
            [TransportHeaders.ConversationOccurredAt] = TransportHeaders.ToHeader(occurredAt),
            [TransportHeaders.AggregateId] = TransportHeaders.ToHeader(message.AggregateId),
            [TransportHeaders.AggregateCorrelationId] = TransportHeaders.ToHeader(message.AggregateCorrelationId),
            [TransportHeaders.AggregateOccurredAt] = TransportHeaders.ToHeader(
                message.AggregateOccurredAt.ToString("O")),
            [TransportHeaders.AggregateConsumers] = TransportHeaders.ToHeader(message.AggregateConsumers),
            [TransportHeaders.RetryCount] = TransportHeaders.ToHeader("0")
        };
    }

    /// <summary>
    /// Builds the envelope continuing an inbound flow: the inbound envelope is cloned from the
    /// <paramref name="transport"/>, the message-level fields are re-stamped for this hop (new id/type/
    /// occurred-at, origin = the inbound destination, destination = this message's exchange, domain
    /// trace from the message), and the conversation is carried over unchanged; the retry counter is
    /// re-stamped to zero — a continuation is a new message with its own retry budget.
    /// </summary>
    private static Dictionary<string, string> Prepare<TMessage>(
        string exchange,
        TMessage message,
        ITransport transport
    )
        where TMessage : ITracedMessage, IFilteredMessage
    {
        if (transport is not Transport inbound)
        {
            throw new InvalidOperationException(
                $"'{transport.GetType().FullName}' is not the RabbitMQ transport; the RabbitMQ bus can only continue a delivery received over RabbitMQ.");
        }

        Guid messageId = Guid.CreateVersion7();
        Type type = message.GetType();

        Dictionary<string, string> headers = inbound.CloneHeaders();

        TransportHeaders.Restamp(headers, TransportHeaders.MessageId, TransportHeaders.ToHeader(messageId));
        TransportHeaders.Restamp(
            headers,
            TransportHeaders.MessageType,
            TransportHeaders.ToHeader(type.FullName ?? type.Name));
        TransportHeaders.Restamp(
            headers,
            TransportHeaders.MessageOriginAddress,
            TransportHeaders.ToHeader(inbound.GetHeaderString(TransportHeaders.MessageDestinationAddress)));
        TransportHeaders.Restamp(
            headers,
            TransportHeaders.MessageDestinationAddress,
            TransportHeaders.ToHeader(exchange));
        TransportHeaders.Restamp(
            headers,
            TransportHeaders.MessageOccurredAt,
            TransportHeaders.ToHeader(DateTime.UtcNow.ToString("O")));
        TransportHeaders.Restamp(headers, TransportHeaders.AggregateId, TransportHeaders.ToHeader(message.AggregateId));
        TransportHeaders.Restamp(
            headers,
            TransportHeaders.AggregateCorrelationId,
            TransportHeaders.ToHeader(message.AggregateCorrelationId));
        TransportHeaders.Restamp(
            headers,
            TransportHeaders.AggregateOccurredAt,
            TransportHeaders.ToHeader(message.AggregateOccurredAt.ToString("O")));
        TransportHeaders.Restamp(
            headers,
            TransportHeaders.AggregateConsumers,
            TransportHeaders.ToHeader(message.AggregateConsumers));
        TransportHeaders.Restamp(headers, TransportHeaders.RetryCount, TransportHeaders.ToHeader(0));

        return headers;
    }
}
