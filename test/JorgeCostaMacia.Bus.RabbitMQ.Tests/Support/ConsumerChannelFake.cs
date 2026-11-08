using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>
/// In-memory inbound gate driving the push consumer deterministically: it captures the callback the
/// worker registers on <see cref="ConsumeAsync"/> and, on <see cref="DeliverAsync"/>, invokes it as the
/// broker would — so a test can push a delivery, await its full processing, and assert. Records the
/// acks and nacks (the ack decision), the consumed queue and the dispose. Serves as its own
/// <see cref="IConsumerChannelFactory"/>, handing itself to the worker first and <see cref="Next"/>
/// on the calls after — the resurrection's replacement.
/// </summary>
internal sealed class ConsumerChannelFake : IConsumerChannel, IConsumerChannelFactory
{
    private Func<BasicDeliverEventArgs, Task>? _onReceived;
    private Func<ShutdownEventArgs?, Task>? _onClosed;

    /// <summary>Whether the channel reports open — flips to false when <see cref="CloseAsync"/> kills it; a test flips
    /// it back to fake the client's automatic recovery reviving it.</summary>
    public bool IsOpen { get; set; } = true;

    /// <summary>The channel the factory hands out on the calls after the first — the resurrection's replacement. Unset,
    /// the factory keeps handing out this fake.</summary>
    public ConsumerChannelFake? Next { get; set; }

    /// <summary>How many channels the factory handed out.</summary>
    public int Created { get; private set; }

    /// <summary>Whether the worker declared its topology on start.</summary>
    public bool Declared { get; private set; }

    /// <summary>The queue the worker subscribed to.</summary>
    public string? ConsumedQueue { get; private set; }

    /// <summary>Whether the worker disposed the channel on stop.</summary>
    public bool Disposed { get; private set; }

    /// <summary>The delivery tags acked by the worker, in order.</summary>
    public List<ulong> Acked { get; } = new List<ulong>();

    /// <summary>The (delivery tag, requeue) nacks by the worker, in order.</summary>
    public List<(ulong DeliveryTag, bool Requeue)> Nacked { get; } = new List<(ulong DeliveryTag, bool Requeue)>();

    /// <inheritdoc />
    public Task<IConsumerChannel> CreateAsync(CancellationToken cancellationToken = default)
    {
        Created++;

        return Task.FromResult<IConsumerChannel>(Created is 1 || Next is null ? this : Next);
    }

    /// <inheritdoc />
    public Task DeclareAsync(
        string exchange,
        string exchangeType,
        string queue,
        ushort prefetchCount,
        CancellationToken cancellationToken = default
    )
    {
        Declared = true;

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ConsumeAsync(
        string queue,
        Func<BasicDeliverEventArgs, Task> onReceived,
        Func<ShutdownEventArgs?, Task> onClosed,
        CancellationToken cancellationToken = default
    )
    {
        ConsumedQueue = queue;
        _onReceived = onReceived;
        _onClosed = onClosed;

        return Task.CompletedTask;
    }

    /// <summary>When set, every ack throws it — the broker-drop-at-ack seam.</summary>
    public Exception? AckFailure { get; set; }

    /// <inheritdoc />
    public Task AckAsync(
        ulong deliveryTag,
        CancellationToken cancellationToken = default
    )
    {
        if (AckFailure is not null)
        {
            throw AckFailure;
        }

        Acked.Add(deliveryTag);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task NackAsync(
        ulong deliveryTag,
        bool requeue,
        CancellationToken cancellationToken = default
    )
    {
        Nacked.Add((deliveryTag, requeue));

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Disposed = true;

        return ValueTask.CompletedTask;
    }

    /// <summary>Drives a delivery into the worker's registered push callback, as the broker would, and awaits its full
    /// processing.</summary>
    /// <param name="args">The delivery to push.</param>
    public Task DeliverAsync(BasicDeliverEventArgs args) => _onReceived?.Invoke(args) ?? Task.CompletedTask;

    /// <summary>
    /// Kills the consumer under the worker, as the broker would, driving the registered death
    /// observer. A shutdown reason closes the channel too; a consumer cancellation (<see langword="null"/>,
    /// e.g. its queue deleted) leaves the channel open but deaf — the real broker semantics.
    /// </summary>
    /// <param name="reason">The shutdown reason, or <see langword="null"/> for a consumer cancellation without
    /// one.</param>
    public Task CloseAsync(ShutdownEventArgs? reason = null)
    {
        if (reason is not null)
        {
            IsOpen = false;
        }

        return _onClosed?.Invoke(reason) ?? Task.CompletedTask;
    }
}
