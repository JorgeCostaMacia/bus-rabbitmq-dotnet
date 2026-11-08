using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// The consume side's inbound gate — the seam over a RabbitMQ channel a <c>ConsumerWorker</c> drives
/// through, the mirror of the <see cref="IProducer"/> gate on the send side. One per worker (a channel
/// is not shared), it exposes only what the worker needs: declare its topology, subscribe a push
/// callback (with an observer for the channel's death), and ack/nack a delivery — so the delivery flow
/// can be driven over a fake without a live broker. Unlike Kafka's pull loop, delivery is push: the
/// broker invokes the callback registered by <see cref="ConsumeAsync"/>.
/// </summary>
internal interface IConsumerChannel : IAsyncDisposable
{
    /// <summary>Whether the channel is open — the resurrection's recovery probe: an open channel after a death means
    /// the client's automatic recovery already revived it.</summary>
    bool IsOpen { get; }

    /// <summary>Declares the worker's topology: the message exchange, the durable queue bound straight to it, and the
    /// prefetch — the <c>.error</c> / <c>.fault</c> park queues are not declared here; they are born lazily on the
    /// first park.</summary>
    /// <param name="exchange">The message exchange the queue binds to.</param>
    /// <param name="exchangeType">The exchange type — <c>direct</c> for commands, <c>fanout</c> for events.</param>
    /// <param name="queue">The queue this worker consumes, bound to the exchange with an empty routing key.</param>
    /// <param name="prefetchCount">The maximum unacked messages the broker delivers before waiting for acks.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task DeclareAsync(
        string exchange,
        string exchangeType,
        string queue,
        ushort prefetchCount,
        CancellationToken cancellationToken = default
    );

    /// <summary>Subscribes a push consumer on the queue — the broker invokes <paramref name="onReceived"/> per
    /// delivery, and <paramref name="onClosed"/> when the channel or the consumer dies.</summary>
    /// <param name="queue">The queue to consume from.</param>
    /// <param name="onReceived">The per-delivery callback.</param>
    /// <param name="onClosed">The death observer — invoked with the shutdown reason when the channel shuts down, or
    /// with <see langword="null"/> when the broker cancels the consumer.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task ConsumeAsync(
        string queue,
        Func<BasicDeliverEventArgs, Task> onReceived,
        Func<ShutdownEventArgs?, Task> onClosed,
        CancellationToken cancellationToken = default
    );

    /// <summary>Acks a delivery — it was dealt with.</summary>
    /// <param name="deliveryTag">The delivery tag to ack.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task AckAsync(
        ulong deliveryTag,
        CancellationToken cancellationToken = default
    );

    /// <summary>Nacks a delivery — <paramref name="requeue"/> decides whether the broker redelivers it.</summary>
    /// <param name="deliveryTag">The delivery tag to nack.</param>
    /// <param name="requeue">Whether the broker requeues the delivery for redelivery.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task NackAsync(
        ulong deliveryTag,
        bool requeue,
        CancellationToken cancellationToken = default
    );
}
