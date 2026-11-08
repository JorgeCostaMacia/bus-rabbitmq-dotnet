using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using ErrorHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors.EventErrorHandlerBase<
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.TestEvent,
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.RecordingEventSubscriber>;
using FaultHandlerBase = JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults.EventFaultHandlerBase<
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.TestEvent,
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.RecordingEventSubscriber>;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure.Consumers.Events;

public sealed class EventWorkerTests
{
    private sealed class BrokerFailure : RabbitMQClientException
    {
        public BrokerFailure() : base("broker down") { }
    }

    private readonly ProducerFake _producer = new ProducerFake();
    private readonly RecordingEventSubscriber _subscriber = new RecordingEventSubscriber();

    private EventWorker<TestEvent, RecordingEventSubscriber> Worker(
        ConsumerChannelFake channel,
        ImmutableList<TimeSpan>? intervals = null
    )
    {
        IServiceProvider provider = new ServiceCollection()
            .AddSingleton(_subscriber)
            .AddScoped<ErrorHandlerBase>(_ =>
                new EventErrorHandler<TestEvent, RecordingEventSubscriber>(
                    _producer,
                    null,
                    NullLogger.Instance,
                    Deliveries.Exchange,
                    Deliveries.Queue,
                    intervals ?? ImmutableList<TimeSpan>.Empty,
                    ImmutableList<Type>.Empty))
            .AddScoped<FaultHandlerBase>(_ =>
                new EventFaultHandler<TestEvent, RecordingEventSubscriber>(
                    _producer,
                    NullLogger.Instance,
                    Deliveries.Queue))
            .BuildServiceProvider();

        return new EventWorker<TestEvent, RecordingEventSubscriber>(
            channel,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<EventWorker<TestEvent, RecordingEventSubscriber>>.Instance,
            Deliveries.Exchange,
            Deliveries.Queue,
            prefetchCount: 10);
    }

    private async Task Deliver(
        ConsumerChannelFake channel,
        EventWorker<TestEvent, RecordingEventSubscriber> worker,
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
    public async Task StartAsync_SubscriberSucceeds_AcksAndProducesNothing()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestEvent("pepe")));

        Assert.Equal("pepe", _subscriber.Received?.Name);
        Assert.True(channel.Declared);
        Assert.Equal(Deliveries.Queue, channel.ConsumedQueue);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
        Assert.Empty(_producer.Produced);
        Assert.True(channel.Disposed);
    }

    [Fact]
    public async Task StartAsync_SubscriberThrowsWithNoLadder_ParksToTheErrorQueueAndAcks()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel), Deliveries.Delivery(new TestEvent("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.Queue}.error", routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
        Assert.Empty(channel.Nacked);
    }

    [Fact]
    public async Task StartAsync_SubscriberThrowsWithAZeroInterval_RepublishesToTheExchangeAndAcks()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel, ImmutableList.Create(TimeSpan.Zero)),
            Deliveries.Delivery(new TestEvent("pepe")));

        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(Deliveries.Exchange, exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_SubscriberThrowsAndTheRequeueHitsABrokerFailure_NacksWithRequeue()
    {
        _subscriber.Failure = new InvalidOperationException("boom");
        _producer.Failure = new BrokerFailure();
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel, ImmutableList.Create(TimeSpan.Zero)),
            Deliveries.Delivery(new TestEvent("pepe")));

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

        Assert.Null(_subscriber.Received);
        Assert.Equal($"{Deliveries.Queue}.fault", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_ANullBody_ParksToTheFaultQueueAndAcks()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(channel, Worker(channel), Deliveries.NullBody());

        Assert.Null(_subscriber.Received);
        Assert.Equal($"{Deliveries.Queue}.fault", Assert.Single(_producer.Produced).RoutingKey);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_AnEventTargetedAtOtherConsumers_SkipsItAndAcks()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel),
            Deliveries.Delivery(new TestEvent("pepe"), consumers: "other.subscriber"));

        Assert.Null(_subscriber.Received);
        Assert.Empty(_producer.Produced);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }

    [Fact]
    public async Task StartAsync_AnEventTargetedAtItsQueue_RunsTheSubscriber()
    {
        ConsumerChannelFake channel = new ConsumerChannelFake();

        await Deliver(
            channel,
            Worker(channel),
            Deliveries.Delivery(new TestEvent("pepe"), consumers: $"other.subscriber, {Deliveries.Queue}"));

        Assert.Equal("pepe", _subscriber.Received?.Name);
        Assert.Equal(10ul, Assert.Single(channel.Acked));
    }
}
