using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>
/// In-memory double of the RabbitMQ <see cref="IChannel"/> the producer publishes through — records
/// every publish (exchange, routing key, persistence, headers, body) and can be told to fail one. Only
/// the publish path the producer uses is implemented; the rest of the wide channel surface throws.
/// </summary>
internal sealed class ChannelFake : IChannel
{
    /// <summary>A captured publish.</summary>
    public sealed record Publish
    {
        public string Exchange { get; init; }
        public string RoutingKey { get; init; }
        public bool Persistent { get; init; }
        public string? MessageId { get; init; }
        public string? CorrelationId { get; init; }
        public string? Type { get; init; }
        public string? AppId { get; init; }
        public string? ContentType { get; init; }
        public long Timestamp { get; init; }
        public IReadOnlyDictionary<string, object?>? Headers { get; init; }
        public ReadOnlyMemory<byte> Body { get; init; }
        public bool Mandatory { get; init; }

        public Publish(
            string exchange,
            string routingKey,
            bool persistent,
            string? messageId,
            string? correlationId,
            string? type,
            string? appId,
            string? contentType,
            long timestamp,
            IReadOnlyDictionary<string, object?>? headers,
            ReadOnlyMemory<byte> body,
            bool mandatory
        )
        {
            Exchange = exchange;
            RoutingKey = routingKey;
            Persistent = persistent;
            MessageId = messageId;
            CorrelationId = correlationId;
            Type = type;
            AppId = appId;
            ContentType = contentType;
            Timestamp = timestamp;
            Headers = headers;
            Body = body;
            Mandatory = mandatory;
        }
    }

    /// <summary>The queues declared through the channel, in order.</summary>
    public List<(string Queue, bool Durable, bool Exclusive, bool AutoDelete)> QueuesDeclared { get; }
        = new List<(string Queue, bool Durable, bool Exclusive, bool AutoDelete)>();

    /// <summary>The publishes handed to the channel, in order. Captured under a lock so concurrent produces record
    /// safely.</summary>
    public List<Publish> Published { get; } = new List<Publish>();

    private readonly object _publishGate = new object();

    /// <summary>An exception to fail every publish with, or <see langword="null"/> to succeed.</summary>
    public Exception? PublishFailure { get; set; }

    /// <summary>The exchanges declared through the channel, in order.</summary>
    public List<(string Exchange, string Type, bool Durable, bool AutoDelete)> ExchangesDeclared { get; }
        = new List<(string Exchange, string Type, bool Durable, bool AutoDelete)>();

    /// <summary>Whether the channel was disposed.</summary>
    public bool Disposed { get; private set; }

    // Explicit implementations — the generic constraint (incl. v7's `allows ref struct`) is inherited from the
    // interface, not restated.

    ValueTask IChannel.BasicPublishAsync<TProperties>(
        string exchange,
        string routingKey,
        bool mandatory,
        TProperties basicProperties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken
    )
    {
        if (PublishFailure is not null)
        {
            return ValueTask.FromException(PublishFailure);
        }

        Publish publish = new Publish(
            exchange,
            routingKey,
            basicProperties.Persistent,
            basicProperties.MessageId,
            basicProperties.CorrelationId,
            basicProperties.Type,
            basicProperties.AppId,
            basicProperties.ContentType,
            basicProperties.Timestamp.UnixTime,
            basicProperties.Headers as IReadOnlyDictionary<string, object?>,
            body,
            mandatory);

        lock (_publishGate)
        {
            Published.Add(publish);
        }

        return ValueTask.CompletedTask;
    }

    ValueTask IChannel.BasicPublishAsync<TProperties>(
        CachedString exchange,
        CachedString routingKey,
        bool mandatory,
        TProperties basicProperties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken
    ) => throw new NotSupportedException();

    // --- Unused channel surface — the producer only publishes. ---

    public int ChannelNumber => throw new NotSupportedException();
    public ShutdownEventArgs? CloseReason => throw new NotSupportedException();
    public IAsyncBasicConsumer? DefaultConsumer
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }
    public bool IsClosed => throw new NotSupportedException();

    /// <summary>Whether the channel is open — set it false to drive the producer's dead-channel replacement.</summary>
    public bool IsOpen { get; set; } = true;
    public string? CurrentQueue => throw new NotSupportedException();
    public TimeSpan ContinuationTimeout
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public event AsyncEventHandler<BasicAckEventArgs> BasicAcksAsync { add { } remove { } }
    public event AsyncEventHandler<BasicNackEventArgs> BasicNacksAsync { add { } remove { } }
    public event AsyncEventHandler<BasicReturnEventArgs> BasicReturnAsync { add { } remove { } }
    public event AsyncEventHandler<CallbackExceptionEventArgs> CallbackExceptionAsync { add { } remove { } }
    public event AsyncEventHandler<ShutdownEventArgs> ChannelShutdownAsync { add { } remove { } }
    public event AsyncEventHandler<FlowControlEventArgs> FlowControlAsync { add { } remove { } }

    public ValueTask BasicAckAsync(
        ulong deliveryTag,
        bool multiple,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public ValueTask BasicNackAsync(
        ulong deliveryTag,
        bool multiple,
        bool requeue,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public ValueTask BasicRejectAsync(
        ulong deliveryTag,
        bool requeue,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task BasicCancelAsync(
        string consumerTag,
        bool noWait,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task<string> BasicConsumeAsync(
        string queue,
        bool autoAck,
        string consumerTag,
        bool noLocal,
        bool exclusive,
        IDictionary<string, object?>? arguments,
        IAsyncBasicConsumer consumer,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task<BasicGetResult?> BasicGetAsync(
        string queue,
        bool autoAck,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task BasicQosAsync(
        uint prefetchSize,
        ushort prefetchCount,
        bool global,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task CloseAsync(
        ushort replyCode,
        string replyText,
        bool abort,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task CloseAsync(
        ShutdownEventArgs reason,
        bool abort,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task CloseAsync(
        ShutdownEventArgs reason,
        bool abort
    ) => throw new NotSupportedException();
    public Task<uint> ConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task ExchangeBindAsync(
        string destination,
        string source,
        string routingKey,
        IDictionary<string, object?>? arguments,
        bool noWait,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task ExchangeDeclareAsync(
        string exchange,
        string type,
        bool durable,
        bool autoDelete,
        IDictionary<string, object?>? arguments,
        bool passive,
        bool noWait,
        CancellationToken cancellationToken = default
    )
    {
        ExchangesDeclared.Add((exchange, type, durable, autoDelete));

        return Task.CompletedTask;
    }
    public Task ExchangeDeclarePassiveAsync(
        string exchange,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task ExchangeDeleteAsync(
        string exchange,
        bool ifUnused,
        bool noWait,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task ExchangeUnbindAsync(
        string destination,
        string source,
        string routingKey,
        IDictionary<string, object?>? arguments,
        bool noWait,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public ValueTask<ulong> GetNextPublishSequenceNumberAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException();
    public Task<uint> MessageCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task QueueBindAsync(
        string queue,
        string exchange,
        string routingKey,
        IDictionary<string, object?>? arguments,
        bool noWait,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task<QueueDeclareOk> QueueDeclareAsync(
        string queue,
        bool durable,
        bool exclusive,
        bool autoDelete,
        IDictionary<string, object?>? arguments,
        bool passive,
        bool noWait,
        CancellationToken cancellationToken = default
    )
    {
        QueuesDeclared.Add((queue, durable, exclusive, autoDelete));

        return Task.FromResult(new QueueDeclareOk(queue, 0, 0));
    }
    public Task<QueueDeclareOk> QueueDeclarePassiveAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task<uint> QueueDeleteAsync(
        string queue,
        bool ifUnused,
        bool ifEmpty,
        bool noWait,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task<uint> QueuePurgeAsync(
        string queue,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task QueueUnbindAsync(
        string queue,
        string exchange,
        string routingKey,
        IDictionary<string, object?>? arguments,
        CancellationToken cancellationToken = default
    ) => throw new NotSupportedException();
    public Task TxCommitAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task TxRollbackAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public Task TxSelectAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public ValueTask DisposeAsync()
    {
        Disposed = true;

        return ValueTask.CompletedTask;
    }

    public void Dispose() => Disposed = true;
}
