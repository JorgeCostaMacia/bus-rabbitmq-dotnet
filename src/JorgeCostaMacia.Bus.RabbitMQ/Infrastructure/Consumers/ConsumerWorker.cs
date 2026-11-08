using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;

/// <summary>
/// The base consumer hosting one handler over one queue: on start it opens its channel from the
/// factory, declares its topology (the message exchange of the right kind and the durable queue bound
/// straight to the exchange — the <c>.error</c> / <c>.fault</c> park queues are born lazily, on the
/// first park), sets the
/// prefetch, and subscribes with a push consumer. Each delivery runs in its own service scope with
/// each failure in its own lane: a malformed delivery (undeserializable body, unreadable envelope)
/// goes to the fault handler; every other handling failure goes to the error handler, and on to the
/// fault handler as the relay when it reports <see cref="ErrorResult.Faulted"/>. A dealt-with delivery
/// is acked; an unresolved one is nacked with requeue to redeliver. The channel is not shared (one per
/// worker), so the push loop is safe. A channel that dies outside a stop is resurrected: after a
/// backoff the worker reopens it from the factory and redeclares — unless the client's automatic
/// recovery already revived it.
/// </summary>
/// <typeparam name="TContext">The context type the handler receives.</typeparam>
/// <typeparam name="THandler">The handler type resolved per delivery.</typeparam>
internal abstract class ConsumerWorker<TContext, THandler> : IHostedService
    where TContext : IContext
    where THandler : IHandler
{
    private static readonly TimeSpan[] _defaultResurrectionBackoff = new TimeSpan[]
    {
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(10),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromSeconds(60)
    };

    private readonly IConsumerChannelFactory _channelFactory;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly string _exchange;
    private readonly string _exchangeType;
    private readonly string _queue;
    private readonly ushort _prefetchCount;
    private readonly CancellationTokenSource _stopping = new CancellationTokenSource();

    private IConsumerChannel? _channel;
    private int _resurrecting;

    /// <summary>Creates the consumer over the channel factory, the scope factory, the logger and its
    /// topology.</summary>
    /// <param name="channelFactory">The factory the worker opens its channel from on start.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="exchange">The message exchange the queue binds to.</param>
    /// <param name="exchangeType">The exchange type — <c>direct</c> for commands, <c>fanout</c> for events.</param>
    /// <param name="queue">The queue this handler consumes.</param>
    /// <param name="prefetchCount">The maximum unacked messages the broker delivers before waiting for acks.</param>
    protected ConsumerWorker(
        IConsumerChannelFactory channelFactory,
        IServiceScopeFactory scopeFactory,
        ILogger logger,
        string exchange,
        string exchangeType,
        string queue,
        ushort prefetchCount
    )
    {
        _channelFactory = channelFactory;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _exchange = exchange;
        _exchangeType = exchangeType;
        _queue = queue;
        _prefetchCount = prefetchCount;
    }

    /// <summary>The queue this worker consumes — the consumer identity used for targeted retries and
    /// filtering.</summary>
    protected string Queue => _queue;

    /// <summary>
    /// The delays before each resurrection attempt, the last one repeating. An instance member rather
    /// than the constant it defaults to only as the test seam: a test shrinks it to milliseconds so the
    /// resurrection runs inside the test, without widening any constructor.
    /// </summary>
    internal IReadOnlyList<TimeSpan> ResurrectionBackoff { get; set; } = _defaultResurrectionBackoff;

    /// <summary>
    /// Consumer-side filtering hook: a filtered delivery is acked and skipped before deserializing the
    /// body. Default: nothing is filtered.
    /// </summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>Whether the delivery is skipped.</returns>
    protected virtual bool Filtered(BasicDeliverEventArgs args) => false;

    /// <summary>Builds the concrete context for a delivery from its args.</summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>The context handed to the handler.</returns>
    protected abstract TContext CreateContext(BasicDeliverEventArgs args);

    /// <summary>Invokes the handler over the delivery's context.</summary>
    /// <param name="handler">The handler resolved from the delivery's scope.</param>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    protected abstract Task Handle(
        THandler handler,
        TContext context,
        CancellationToken cancellationToken
    );

    /// <summary>Hands a handling failure to the error handler resolved from the delivery's scope, and reports its
    /// verdict.</summary>
    /// <param name="services">The delivery's scoped service provider.</param>
    /// <param name="context">The delivery's context (the handler already ran).</param>
    /// <param name="exception">The failure the handler raised.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    protected abstract Task<ErrorResult> HandleError(
        IServiceProvider services,
        TContext context,
        Exception exception,
        CancellationToken cancellationToken
    );

    /// <summary>Hands a broken (or relayed) delivery to the fault handler resolved from the delivery's scope, and
    /// reports its verdict.</summary>
    /// <param name="services">The delivery's scoped service provider.</param>
    /// <param name="body">The delivered raw body, never deserialized.</param>
    /// <param name="transport">The broken delivery's transport.</param>
    /// <param name="exception">The failure that broke the delivery.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    protected abstract Task<FaultResult> HandleFault(
        IServiceProvider services,
        ReadOnlyMemory<byte> body,
        Transport transport,
        Exception exception,
        CancellationToken cancellationToken
    );

    /// <summary>Opens the channel, declares the topology (message exchange + queue bound to it), and starts consuming —
    /// the <c>.error</c> / <c>.fault</c> park queues are born lazily, on the first park.</summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        IConsumerChannel channel = await _channelFactory.CreateAsync(cancellationToken);
        _channel = channel;

        await channel.DeclareAsync(_exchange, _exchangeType, _queue, _prefetchCount, cancellationToken);
        await channel.ConsumeAsync(_queue, args => OnReceivedAsync(channel, args), OnClosedAsync, cancellationToken);
    }

    /// <summary>
    /// The channel (or its consumer) died under the worker: the death is logged with the shutdown
    /// reason in the context, nothing is torn down, and a single resurrection is started — overlapping
    /// death events fold into the one already running. A clean stop stays silent and resurrects nothing.
    /// </summary>
    /// <param name="reason">The shutdown reason, or <see langword="null"/> when the broker cancelled the consumer
    /// without one.</param>
    private Task OnClosedAsync(ShutdownEventArgs? reason)
    {
        if (_stopping.IsCancellationRequested)
        {
            return Task.CompletedTask;
        }

        // our own close, not a death: disposing the dead channel after a resurrection raises its
        // shutdown event one last time — logging it would stamp a spurious warning per resurrection.
        if (reason?.Initiator == ShutdownInitiator.Application)
        {
            return Task.CompletedTask;
        }

        using (BusLogger.WorkerContext(_exchange, _queue))
        using (BusLogger.ShutdownContext(reason))
        using (BusLogger.DescriptionContext(BusLoggerDescriptions.ConsumerChannelClosed))
        {
            _logger.LogWarning("Channel closed.");
        }

        if (Interlocked.CompareExchange(ref _resurrecting, 1, 0) == 0)
        {
            _ = ResurrectAsync(consumerCancelled: reason is null);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Resurrects the dead channel — wait, check, then act. The client's automatic recovery already
    /// restores channels and consumers after a connection-level drop (the same channel comes back open
    /// with its consumer re-registered), but not a channel closed by a channel-level exception nor a
    /// consumer the broker cancelled — and resurrecting blindly would double-subscribe when recovery
    /// also acts. So each pass waits out a backoff step first, then: a stop ends the loop; after a
    /// channel shutdown, a channel recovery reopened is left alone; a <b>cancelled consumer</b>
    /// (e.g. its queue deleted) leaves the channel open but deaf, so it always rebuilds — the open
    /// channel proves nothing there. The rebuild is a new channel from the factory, same topology,
    /// same handler, the old one disposed. A failed attempt just waits for the next step, silently:
    /// the death already logged its warning, and per-attempt noise groups nothing.
    /// </summary>
    /// <param name="consumerCancelled">Whether the death was the broker cancelling the consumer (no shutdown reason) —
    /// the channel stays open, so the recovery check is skipped.</param>
    private async Task ResurrectAsync(bool consumerCancelled)
    {
        try
        {
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    await Task.Delay(
                        ResurrectionBackoff[Math.Min(attempt, ResurrectionBackoff.Count - 1)],
                        _stopping.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                if (_stopping.IsCancellationRequested)
                {
                    return;
                }

                if (!consumerCancelled && _channel!.IsOpen)
                {
                    return;
                }

                try
                {
                    IConsumerChannel channel = await _channelFactory.CreateAsync(_stopping.Token);

                    await channel.DeclareAsync(_exchange, _exchangeType, _queue, _prefetchCount, _stopping.Token);
                    await channel.ConsumeAsync(
                        _queue,
                        args => OnReceivedAsync(channel, args),
                        OnClosedAsync,
                        _stopping.Token);

                    IConsumerChannel dead = _channel!;
                    _channel = channel;

                    await dead.DisposeAsync();

                    using (BusLogger.WorkerContext(_exchange, _queue))
                    using (BusLogger.DescriptionContext(BusLoggerDescriptions.ConsumerChannelRestored))
                    {
                        _logger.LogInformation("Channel restored.");
                    }

                    return;
                }
                catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception)
                {
                    // the attempt failed — the next backoff step retries; the death already logged.
                }
            }
        }
        finally
        {
            Interlocked.Exchange(ref _resurrecting, 0);
        }
    }

    /// <summary>
    /// One delivery inside its own service scope: build the context, run the handler resolved from the
    /// scope, then ack. The two steps fail into different lanes — a context that cannot be built is a
    /// malformed delivery and goes to the fault handler, while any exception thrown by the handler
    /// itself (whatever its type) goes to the error handler and its retry ladder (which still relays to
    /// the fault handler on <see cref="ErrorResult.Faulted"/>). A shutdown cancellation leaves the
    /// delivery unacked for the broker to requeue. Ack/nack target <paramref name="channel"/> — the
    /// channel that delivered this message, captured in the consume closure — so a resurrection swapping
    /// the field mid-flight can never cross-ack the fresh channel with this channel's delivery tag.
    /// </summary>
    /// <param name="channel">The channel that delivered this message — the one every ack/nack targets.</param>
    /// <param name="args">The delivered message.</param>
    private async Task OnReceivedAsync(
        IConsumerChannel channel,
        BasicDeliverEventArgs args
    )
    {
        using IDisposable workerScope = BusLogger.WorkerContext(_exchange, _queue);
        using IDisposable deliveryScope = BusLogger.ConsumerContext(args);

        await using AsyncServiceScope scope = _scopeFactory.CreateAsyncScope();

        if (Filtered(args))
        {
            await AckQuietly(channel, args);

            return;
        }

        TContext context;

        try
        {
            context = CreateContext(args);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // Shutting down — leave the delivery unacked for the broker to requeue on channel close.
            return;
        }
        catch (Exception exception)
        {
            await Fault(channel, scope.ServiceProvider, args, exception);

            return;
        }

        try
        {
            await Handle(scope.ServiceProvider.GetRequiredService<THandler>(), context, _stopping.Token);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // Shutting down — leave the delivery unacked for the broker to requeue on channel close.
            return;
        }
        catch (Exception exception)
        {
            await Error(channel, scope.ServiceProvider, args, context, exception);

            return;
        }

        await AckQuietly(channel, args);
    }

    /// <summary>
    /// Acks a delivery whose work is already done (handled or filtered). An ack failure must never
    /// reach the error lane — its re-publish would duplicate the work — so it is only logged: left
    /// unacked, the broker redelivers on channel recovery and the idempotent handler absorbs it.
    /// </summary>
    /// <param name="channel">The channel that delivered the message — the ack targets it, never the field.</param>
    /// <param name="args">The delivery to ack.</param>
    private async Task AckQuietly(
        IConsumerChannel channel,
        BasicDeliverEventArgs args
    )
    {
        try
        {
            await Ack(channel, args);
        }
        catch (Exception exception)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.RedeliveredOnRecovery))
            {
                _logger.LogWarning(exception, "Ack failed.");
            }
        }
    }

    /// <summary>
    /// A handling failure: hands the delivery to the error handler and acts on its verdict — a requeue,
    /// schedule or park acks it; a report of <see cref="ErrorResult.Faulted"/> (or a context that never
    /// built) relays to the fault handler; anything else nacks with requeue to redeliver. An error
    /// handler that itself throws leaves the delivery nacked-with-requeue rather than tearing down.
    /// Ack/nack target <paramref name="channel"/> — the channel that delivered the message.
    /// </summary>
    /// <param name="channel">The channel that delivered the message — every ack/nack here targets it.</param>
    /// <param name="services"></param>
    /// <param name="args"></param>
    /// <param name="context"></param>
    /// <param name="exception"></param>
    private async Task Error(
        IConsumerChannel channel,
        IServiceProvider services,
        BasicDeliverEventArgs args,
        TContext? context,
        Exception exception
    )
    {
        ErrorResult outcome;

        try
        {
            outcome = context is not null
                ? await HandleError(services, context, exception, _stopping.Token)
                : ErrorResult.Faulted;
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // shutting down, not a failure: the delivery stays unacked and the broker redelivers it.
            return;
        }
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.NackedWithRequeue))
            {
                _logger.LogError(failure, "Error handler failed.");
            }

            await Nack(channel, args);

            return;
        }

        switch (outcome)
        {
            // the retry/park is already published: an ack failure here must not undo that work —
            // quietly logged, the broker redelivers and the idempotent handling absorbs it.
            case ErrorResult.Retried or ErrorResult.Scheduled or ErrorResult.Parked:
                await AckQuietly(channel, args);
                break;

            case ErrorResult.Faulted:
                await Fault(channel, services, args, exception);
                break;

            default:
                await Nack(channel, args);
                break;
        }
    }

    /// <summary>
    /// A broken (or relayed) delivery: hands it to the fault handler and acks it when the handler parks
    /// it; an unparked delivery is nacked with requeue to redeliver. Never throws for control flow — a
    /// fault handler that itself fails leaves the delivery nacked-with-requeue rather than tearing down.
    /// Ack/nack target <paramref name="channel"/> — the channel that delivered the message.
    /// </summary>
    /// <param name="channel">The channel that delivered the message — every ack/nack here targets it.</param>
    /// <param name="services"></param>
    /// <param name="args"></param>
    /// <param name="exception"></param>
    private async Task Fault(
        IConsumerChannel channel,
        IServiceProvider services,
        BasicDeliverEventArgs args,
        Exception exception
    )
    {
        FaultResult outcome;

        try
        {
            outcome = await HandleFault(services, args.Body, Transport.Create(args), exception, _stopping.Token);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // shutting down, not a failure: the delivery stays unacked and the broker redelivers it.
            return;
        }
        catch (Exception failure)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.NackedWithRequeue))
            {
                _logger.LogError(failure, "Fault handler failed.");
            }

            await Nack(channel, args);

            return;
        }

        // the fault is already parked: an ack failure here must not undo that work (a nack would
        // redeliver and park a duplicate) — quietly logged, the broker redelivers on recovery.
        if (outcome is FaultResult.Parked)
        {
            await AckQuietly(channel, args);
        }
        else
        {
            await Nack(channel, args);
        }
    }

    /// <summary>Acks the delivery on the channel that delivered it — it was dealt with; the delivering channel is
    /// targeted so a resurrection swap can't cross-ack.</summary>
    /// <param name="channel">The channel that delivered the message.</param>
    /// <param name="args">The delivery to ack.</param>
    private async Task Ack(
        IConsumerChannel channel,
        BasicDeliverEventArgs args
    ) => await channel.AckAsync(args.DeliveryTag, _stopping.Token);

    /// <summary>
    /// Nacks the delivery with requeue on the channel that delivered it — it was not dealt with and
    /// redelivers. A requeued delivery comes back immediately, so a persistent failure would spin hot
    /// (× prefetch): the nack waits a second first (like the Kafka worker's consume backoff), skipped
    /// when stopping so shutdown doesn't linger. Targeting the delivering channel keeps a resurrection
    /// swap from nacking the fresh channel with this channel's tag. A nack failure is tolerated like an
    /// ack failure: if that channel already died the nack throws, but the unacked delivery redelivers on
    /// recovery anyway — so it is swallowed and logged rather than surfaced as a client callback fault.
    /// </summary>
    /// <param name="channel">The channel that delivered the message.</param>
    /// <param name="args">The delivery to nack.</param>
    private async Task Nack(
        IConsumerChannel channel,
        BasicDeliverEventArgs args
    )
    {
        if (!_stopping.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken.None);
        }

        try
        {
            await channel.NackAsync(args.DeliveryTag, requeue: true, _stopping.Token);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // shutting down — leave the delivery unacked for the broker to requeue on channel close.
        }
        catch (Exception exception)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.RedeliveredOnRecovery))
            {
                _logger.LogWarning(exception, "Nack failed.");
            }
        }
    }

    /// <summary>
    /// Stops consuming and closes the channel — whichever the field holds at that moment: a resurrection
    /// still mid-flight exits on the stopping token. The stop is signalled and the channel disposed, but the
    /// token source is deliberately not disposed: a delivery still in flight reads it (its cancellation
    /// is what makes it leave the delivery unacked to redeliver), and disposing it under that read is a
    /// shutdown-time race for no gain — a worker is an app-lifetime singleton, so the token source holds
    /// nothing worth eagerly reclaiming.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync();

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        using (BusLogger.WorkerContext(_exchange, _queue))
        using (BusLogger.DescriptionContext(BusLoggerDescriptions.WorkerStopped))
        {
            _logger.LogInformation("Worker stopped.");
        }
    }
}
