using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using ErrorHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors.CommandErrorHandlerBase<
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.TestCommand,
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.RecordingCommandHandler>;
using FaultHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Faults.CommandFaultHandlerBase<
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.TestCommand,
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.RecordingCommandHandler>;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure.Consumers.Commands;

public sealed class CommandWorkerTests
{
    private sealed class BrokerFailure : RabbitMQClientException
    {
        public BrokerFailure() : base("broker down") { }
    }

    private readonly ProducerFake _producer = new ProducerFake();
    private readonly RecordingCommandHandler _handler = new RecordingCommandHandler();

    private CommandWorker<TestCommand, RecordingCommandHandler> Worker(
        ConsumerChannelFake channel,
        ImmutableList<TimeSpan>? intervals = null,
        RetrySchedulerFake? scheduler = null,
        ErrorHandlerBase? errorHandler = null,
        FaultHandlerBase? faultHandler = null,
        ILogger<CommandWorker<TestCommand, RecordingCommandHandler>>? logger = null
    )
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_handler)
            .AddScoped<ErrorHandlerBase>(_ =>
                errorHandler
                    ?? new CommandErrorHandler<TestCommand, RecordingCommandHandler>(
                        _producer,
                        scheduler,
                        NullLogger.Instance,
                        Deliveries.Exchange,
                        Deliveries.Queue,
                        intervals ?? ImmutableList<TimeSpan>.Empty,
                        ImmutableList<Type>.Empty))
            .AddScoped<FaultHandlerBase>(_ =>
                faultHandler
                    ?? new CommandFaultHandler<TestCommand, RecordingCommandHandler>(
                        _producer,
                        NullLogger.Instance,
                        Deliveries.Queue))
            .BuildServiceProvider();

        return new CommandWorker<TestCommand, RecordingCommandHandler>(
            channel,
            provider.GetRequiredService<IServiceScopeFactory>(),
            logger ?? NullLogger<CommandWorker<TestCommand, RecordingCommandHandler>>.Instance,
            Deliveries.Exchange,
            Deliveries.Queue,
            prefetchCount: 10);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));

        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    private async Task Deliver(
        ConsumerChannelFake channel,
        CommandWorker<TestCommand, RecordingCommandHandler> worker,
        params BasicDeliverEventArgs[] deliveries
    )
    {
        await worker.StartAsync(TestContext.Current.CancellationToken);

        foreach (BasicDeliverEventArgs delivery in deliveries)
        {
            await channel.DeliverAsync(delivery);
        }

        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StartAsync_HandlerSucceeds_AcksAndProducesNothing()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.True(channel.Declared);
        Assert.Equal(Deliveries.Queue, channel.ConsumedQueue);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
        Assert.Empty(_producer.Produced);
        Assert.True(channel.Disposed);
    }

    [Fact]
    public async Task StartAsync_AckFailsAfterTheHandlerSucceeds_NeitherParksNorNacks()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake() { AckFailure = new BrokerFailure() };

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Empty(_producer.Produced);   // never routed to the error lane — that would duplicate the work
        Assert.Empty(channel.Nacked);       // left unacked: the broker redelivers on recovery
    }

    [Fact]
    public async Task StartAsync_ErrorHandlerThrows_NacksWithRequeueWithoutTearingDown()
    {
        // the error lane's endgame: the error handler itself breaking returns the delivery to the
        // broker (nack with requeue) instead of tearing down the worker.
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel, errorHandler: new ThrowingCommandErrorHandler()),
            Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Empty(channel.Acked);
        Assert.Empty(_producer.Produced);
        (ulong deliveryTag, bool requeue) = Assert.Single(channel.Nacked);
        Assert.Equal(10ul, deliveryTag);
        Assert.True(requeue);
    }

    [Fact]
    public async Task StartAsync_FaultHandlerThrows_NacksWithRequeueWithoutTearingDown()
    {
        // the last net's endgame: even the fault handler breaking returns the delivery to the
        // broker (nack with requeue) instead of tearing down the worker.
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel, faultHandler: new ThrowingCommandFaultHandler()), Deliveries.Garbage());

        Assert.Empty(channel.Acked);
        Assert.Empty(_producer.Produced);
        (ulong deliveryTag, bool requeue) = Assert.Single(channel.Nacked);
        Assert.Equal(10ul, deliveryTag);
        Assert.True(requeue);
    }

    [Fact]
    public async Task StartAsync_AckFailsAfterAnErrorPark_NeitherNacksNorParksTwice()
    {
        // the park to .error succeeded and only the ack failed: a nack would redeliver and park a
        // duplicate — the delivery stays unacked (logged) for the broker to redeliver on recovery.
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new ConsumerChannelFake() { AckFailure = new BrokerFailure() };

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Equal($"{Deliveries.Queue}.error", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task StartAsync_AckFailsAfterAFaultPark_NeitherNacksNorParksTwice()
    {
        // the same isolation for the fault lane: the park to .fault exists, only the receipt failed.
        ConsumerChannelFake channel = new ConsumerChannelFake() { AckFailure = new BrokerFailure() };

        await Deliver(channel, Worker(channel), Deliveries.Garbage());

        Assert.Equal($"{Deliveries.Queue}.fault", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task StartAsync_HandlerThrowsWithNoLadder_ParksToTheErrorQueueAndAcks()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestCommand("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.Queue}.error", routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task StartAsync_HandlerThrowsWithAZeroInterval_RepublishesToTheExchangeAndAcks()
    {
        _handler.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel, ImmutableList.Create(TimeSpan.Zero)),
            Deliveries.Delivery(new TestCommand("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.Exchange, exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_HandlerThrowsWithAPositiveInterval_SchedulesTheRetryAndAcks()
    {
        // the delayed retry is parked through the scheduler, not published: the delivery is done —
        // acked like a requeue or a park.
        _handler.Failure = new InvalidOperationException("boom");
        RetrySchedulerFake scheduler = new RetrySchedulerFake();
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel, ImmutableList.Create(TimeSpan.FromMinutes(5)), scheduler),
            Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Single(scheduler.Scheduled);
        Assert.Empty(_producer.Produced);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task StartAsync_HandlerThrowsAndTheRequeueHitsABrokerFailure_NacksWithRequeue()
    {
        _handler.Failure = new InvalidOperationException("boom");
        _producer.Failure = new BrokerFailure();
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel, ImmutableList.Create(TimeSpan.Zero)),
            Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Empty(channel.Acked);
        (ulong deliveryTag, bool requeue) = Assert.Single(channel.Nacked);
        Assert.Equal(10ul, deliveryTag);
        Assert.True(requeue);
    }

    [Fact]
    public async Task StartAsync_AMalformedBody_ParksToTheFaultQueueAndAcks()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel), Deliveries.Garbage());

        Assert.Null(_handler.Received);
        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.Queue}.fault", routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_ANullBody_ParksToTheFaultQueueAndAcks()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel), Deliveries.NullBody());

        Assert.Null(_handler.Received);
        Assert.Equal($"{Deliveries.Queue}.fault", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_SeveralDeliveries_HandlesAndAcksEachInOrder()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel),
            Deliveries.Delivery(new TestCommand("a"), 10),
            Deliveries.Delivery(new TestCommand("b"), 11));

        Assert.Equal("b", _handler.Received?.Name);
        Assert.Equal(new ulong[] { 10ul, 11ul }, channel.Acked);
    }

    [Fact]
    public async Task StartAsync_ACommandTargetedAtAnotherConsumer_HandlesItAnyway()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel),
            Deliveries.Delivery(new TestCommand("pepe"), consumers: "other.handler"));

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_ThePeerClosesTheChannel_WarnsWithoutTearingDown()
    {
        // the broker closes the channel under the worker: visibility only — the death is logged as a
        // warning with the shutdown reason, nothing is torn down, and the worker still stops cleanly.
        ConsumerChannelFake channel = new ConsumerChannelFake();
        RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>> logger
            = new RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>>();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, logger: logger);

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(new ShutdownEventArgs(ShutdownInitiator.Peer, 541, "INTERNAL_ERROR"));

        Assert.Equal((LogLevel.Warning, "Channel closed."), Assert.Single(logger.Logged));
        Assert.False(channel.Disposed);

        await worker.StopAsync(TestContext.Current.CancellationToken);

        Assert.True(channel.Disposed);
        Assert.Empty(channel.Acked);
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task StartAsync_AnApplicationCloseMidRun_StaysSilentAndDoesNotResurrect()
    {
        // the worker's own dispose of a replaced channel raises the shutdown event with the
        // Application initiator: not a death — no warning, no resurrection. Observed live: every
        // resurrection used to log its own funeral as a spurious "Channel closed." warning.
        ConsumerChannelFake channel = new ConsumerChannelFake();
        RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>> logger
            = new RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>>();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, logger: logger);
        worker.ResurrectionBackoff = new TimeSpan[] { TimeSpan.FromMilliseconds(1) };

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Goodbye"));
        await Task.Delay(100, TestContext.Current.CancellationToken);

        Assert.DoesNotContain(logger.Logged, log => log.Message == "Channel closed.");
        Assert.Equal(1, channel.Created);

        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StopAsync_AChannelCloseAfterTheStop_LogsNoWarning()
    {
        // the channel's own dispose raises the shutdown event on a clean stop: not a death — the
        // worker is already stopping and logs no warning.
        ConsumerChannelFake channel = new ConsumerChannelFake();
        RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>> logger
            = new RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>>();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, logger: logger);

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await worker.StopAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Goodbye"));

        Assert.DoesNotContain(logger.Logged, log => log.Message == "Channel closed.");
    }

    [Fact]
    public async Task StartAsync_TheChannelDiesAndStaysClosed_ResurrectsOnANewChannel()
    {
        // a channel-level death automatic recovery never repairs (e.g. a 404/406): after the backoff
        // the worker opens a new channel from the factory, redeclares the same topology, consumes with
        // the same handler, and disposes the dead one.
        ConsumerChannelFake replacement = new ConsumerChannelFake();
        ConsumerChannelFake channel = new ConsumerChannelFake() { Next = replacement };
        RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>> logger
            = new RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>>();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, logger: logger);
        worker.ResurrectionBackoff = new TimeSpan[] { TimeSpan.FromMilliseconds(1) };

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(new ShutdownEventArgs(ShutdownInitiator.Peer, 404, "NOT_FOUND"));
        await WaitUntil(() => channel.Disposed);
        await WaitUntil(() => logger.Logged.Count >= 2);

        Assert.Equal(2, channel.Created);
        Assert.True(replacement.Declared);
        Assert.Equal(Deliveries.Queue, replacement.ConsumedQueue);
        Assert.Contains((LogLevel.Information, "Channel restored."), logger.Logged);
        Assert.False(replacement.Disposed);

        await replacement.DeliverAsync(Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(10ul, Assert.Single(replacement.Acked));

        await worker.StopAsync(TestContext.Current.CancellationToken);

        Assert.True(replacement.Disposed);
    }

    [Fact]
    public async Task StartAsync_ADeliveryOnTheDeadChannelAfterResurrection_AcksOnThatChannel()
    {
        // the concurrency bug: a channel dies and is resurrected, swapping the worker's _channel field
        // to a fresh channel — but a delivery that arrived on the dead channel must ack on THAT channel
        // (the one captured in its consume closure), never on the field. Acking the fresh channel with
        // the dead channel's delivery tag is a mis-ack: silent message loss, or a 406 that kills the
        // fresh channel. The swap and the delivery use different channel instances, so a per-instance
        // ack assertion pins the target: the field is 'resurrected', the delivery arrived on 'delivering'.
        ConsumerChannelFake resurrected = new ConsumerChannelFake();
        ConsumerChannelFake delivering = new ConsumerChannelFake() { Next = resurrected };
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(delivering);
        worker.ResurrectionBackoff = new TimeSpan[] { TimeSpan.FromMilliseconds(1) };

        await worker.StartAsync(TestContext.Current.CancellationToken);

        // the delivering channel dies and the worker resurrects onto the fresh channel: the _channel
        // field now points at 'resurrected', while 'delivering' still holds the in-flight delivery's
        // captured callback — exactly the state a slow handler sees when its channel is swapped mid-flight.
        await delivering.CloseAsync(new ShutdownEventArgs(ShutdownInitiator.Peer, 406, "PRECONDITION_FAILED"));
        await WaitUntil(() => delivering.Disposed && resurrected.Declared);

        await delivering.DeliverAsync(Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Equal("pepe", _handler.Received?.Name);
        Assert.Equal(10ul, Assert.Single(delivering.Acked)); // acked on the channel that DELIVERED it
        Assert.Empty(resurrected.Acked);                     // never cross-acked the resurrected channel
        Assert.Empty(delivering.Nacked);
        Assert.Empty(resurrected.Nacked);

        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StartAsync_BrokerCancelsTheConsumerOnAnOpenChannel_ResurrectsAnyway()
    {
        // the broker cancelling the consumer (e.g. its queue deleted) leaves the channel OPEN but
        // deaf — the open channel proves nothing, so the resurrection must rebuild regardless.
        // Pinned live against the real broker: the open-channel check used to swallow this death.
        ConsumerChannelFake replacement = new ConsumerChannelFake();
        ConsumerChannelFake channel = new ConsumerChannelFake() { Next = replacement };
        RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>> logger
            = new RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>>();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, logger: logger);
        worker.ResurrectionBackoff = new TimeSpan[] { TimeSpan.FromMilliseconds(1) };

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(null);
        await WaitUntil(() => channel.Disposed);

        Assert.True(channel.IsOpen);   // the deaf channel really was open the whole time
        Assert.Equal(2, channel.Created);
        Assert.True(replacement.Declared);
        Assert.Equal(Deliveries.Queue, replacement.ConsumedQueue);

        await replacement.DeliverAsync(Deliveries.Delivery(new TestCommand("pepe")));

        Assert.Equal("pepe", _handler.Received?.Name);

        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StartAsync_TheChannelDiesButRecoveryRevivesIt_DoesNotResurrect()
    {
        // a connection-level drop: the client's automatic recovery brings the SAME channel back open
        // with its consumer re-registered — the resurrection checks before acting and leaves it alone,
        // where a blind reopen would double-subscribe.
        ConsumerChannelFake channel = new ConsumerChannelFake();
        RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>> logger
            = new RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>>();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, logger: logger);
        worker.ResurrectionBackoff = new TimeSpan[] { TimeSpan.FromMilliseconds(50) };

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(new ShutdownEventArgs(ShutdownInitiator.Library, 320, "CONNECTION_FORCED"));
        channel.IsOpen = true; // automatic recovery revived it before the first backoff elapsed

        await Task.Delay(TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);

        Assert.Equal(1, channel.Created);
        Assert.False(channel.Disposed);
        Assert.DoesNotContain(logger.Logged, log => log.Message == "Channel restored.");

        await worker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StopAsync_AChannelCloseAfterTheStop_DoesNotResurrect()
    {
        // the shutdown event racing a stop: the worker is already stopping — no warning, and no
        // resurrection either.
        ConsumerChannelFake channel = new ConsumerChannelFake();
        RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>> logger
            = new RecordingLogger<CommandWorker<TestCommand, RecordingCommandHandler>>();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, logger: logger);
        worker.ResurrectionBackoff = new TimeSpan[] { TimeSpan.FromMilliseconds(1) };

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await worker.StopAsync(TestContext.Current.CancellationToken);
        await channel.CloseAsync(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Goodbye"));

        await Task.Delay(TimeSpan.FromMilliseconds(100), TestContext.Current.CancellationToken);

        Assert.Equal(1, channel.Created);
        Assert.DoesNotContain(logger.Logged, log => log.Message == "Channel restored.");
    }

    [Fact]
    public async Task StartAsync_ShutdownDuringErrorHandling_LeavesItUnackedWithoutNack()
    {
        // the app shuts down while the error lane runs: the cancellation is not a failure — the
        // delivery is left unacked, without nack, for the broker to requeue on channel close.
        _handler.Failure = new InvalidOperationException("boom");
        StoppingCommandErrorHandler errorHandler = new StoppingCommandErrorHandler();
        ConsumerChannelFake channel = new ConsumerChannelFake();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, errorHandler: errorHandler);
        errorHandler.Stop = () => worker.StopAsync(TestContext.Current.CancellationToken);

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await channel.DeliverAsync(Deliveries.Delivery(new TestCommand("pepe")));
        await errorHandler.Stopping!.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Empty(channel.Acked);
        Assert.Empty(channel.Nacked);
        Assert.Empty(_producer.Produced);
        Assert.True(channel.Disposed);
    }

    [Fact]
    public async Task StartAsync_ShutdownDuringAFaultPark_LeavesItUnackedWithoutNack()
    {
        // the app shuts down while the fault lane is parking a broken delivery: the cancellation is
        // not a failure — the delivery is left unacked, without nack, for the broker to requeue.
        StoppingCommandFaultHandler faultHandler = new StoppingCommandFaultHandler();
        ConsumerChannelFake channel = new ConsumerChannelFake();
        CommandWorker<TestCommand, RecordingCommandHandler> worker = Worker(channel, faultHandler: faultHandler);
        faultHandler.Stop = () => worker.StopAsync(TestContext.Current.CancellationToken);

        await worker.StartAsync(TestContext.Current.CancellationToken);
        await channel.DeliverAsync(Deliveries.Garbage());
        await faultHandler.Stopping!.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Empty(channel.Acked);
        Assert.Empty(channel.Nacked);
        Assert.Empty(_producer.Produced);
        Assert.True(channel.Disposed);
    }
}
