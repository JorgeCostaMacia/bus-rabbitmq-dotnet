using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using Microsoft.Extensions.Logging;
using RabbitProducer = JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers.Producer;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure.Producers;

public sealed class ProducerTests
{
    private readonly ChannelFake _channel = new ChannelFake();
    private readonly ConnectionFake _connection;
    private readonly RecordingLogger<RabbitProducer> _logger = new RecordingLogger<RabbitProducer>();

    public ProducerTests()
    {
        _connection = new ConnectionFake(_channel);
    }

    private RabbitProducer Sut() => new RabbitProducer(_connection, _logger);

    private static Dictionary<string, string> Headers() => new Dictionary<string, string>();

    [Fact]
    public async Task Produce_AnExchangeAndARoutingKey_PublishesToThem()
    {
        await Sut().Produce("orders", "rk", "hi"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        ChannelFake.Publish publish = Assert.Single(_channel.Published);
        Assert.Equal("orders", publish.Exchange);
        Assert.Equal("rk", publish.RoutingKey);
        Assert.Equal("hi", Encoding.UTF8.GetString(publish.Body.Span));
    }

    [Fact]
    public async Task Produce_ToAnExchange_MarksTheMessagePersistent()
    {
        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.True(Assert.Single(_channel.Published).Persistent);
    }

    [Fact]
    public async Task Produce_SameExchangeTwice_ReusesTheDestinationChannel()
    {
        // one long-lived channel per destination: the second publish to the same exchange rides the
        // cached channel instead of opening another.
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);
        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.Equal(1, _connection.Created);
    }

    [Fact]
    public async Task Produce_DifferentExchanges_OpenOneChannelEach()
    {
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);
        await sut.Produce(
            "orders.created",
            string.Empty,
            "{}"u8.ToArray(),
            Headers(),
            TestContext.Current.CancellationToken);

        Assert.Equal(2, _connection.Created);
    }

    [Fact]
    public async Task Produce_ChannelDied_ReopensInsteadOfReusingTheDeadOne()
    {
        // an async publish error closes the channel broker-side: the next produce to that exchange
        // must replace it instead of handing out the dead cached one for the application's lifetime.
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.Equal(1, _connection.Created);

        _channel.IsOpen = false;
        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.Equal(2, _connection.Created);
    }

    [Fact]
    public async Task DisposeAsync_AfterAProduce_ClosesEveryDestinationChannel()
    {
        RabbitProducer sut = Sut();

        await sut.Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);
        await sut.DisposeAsync();

        Assert.True(_channel.Disposed);
    }

    [Fact]
    public async Task Park_ToAParkQueue_RedeclaresItAndPublishesMandatoryThroughTheDefaultExchange()
    {
        // the loss-proof park: the idempotent declare recreates a park queue deleted at runtime
        // (the consumers' exact options), and mandatory makes an unroutable park throw instead of
        // being dropped — and confirmed — silently.
        await Sut().Park("orders.handler.error", "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        (string queue, bool durable, bool exclusive, bool autoDelete) = Assert.Single(_channel.QueuesDeclared);
        Assert.Equal("orders.handler.error", queue);
        Assert.True(durable);
        Assert.False(exclusive);
        Assert.False(autoDelete);

        ChannelFake.Publish publish = Assert.Single(_channel.Published);
        Assert.Equal(string.Empty, publish.Exchange);
        Assert.Equal("orders.handler.error", publish.RoutingKey);
        Assert.True(publish.Mandatory);
    }

    [Fact]
    public async Task Produce_ToAnExchange_PublishesWithoutMandatory()
    {
        // an unroutable normal publish is legitimate (a fanout with no subscribers) — only parks trip.
        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        Assert.False(Assert.Single(_channel.Published).Mandatory);
    }

    [Fact]
    public async Task Produce_Failure_LogsAndRethrows()
    {
        _channel.PublishFailure = new InvalidOperationException("broker down");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => Sut().Produce(
                "orders",
                string.Empty,
                "{}"u8.ToArray(),
                Headers(),
                TestContext.Current.CancellationToken));

        (LogLevel level, string message) = Assert.Single(_logger.Logged);
        Assert.Equal(LogLevel.Error, level);
        Assert.Equal("ProducerFailed", message);
    }

    [Fact]
    public async Task Produce_AnEnvelopeWithoutHostHeaders_StampsTheHost()
    {
        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), Headers(), TestContext.Current.CancellationToken);

        IReadOnlyDictionary<string, object?> headers = Assert.Single(_channel.Published).Headers!;
        Assert.Equal(Environment.MachineName, Header(headers, TransportHeaders.HostMachineName));
        Assert.False(string.IsNullOrWhiteSpace(Header(headers, TransportHeaders.HostAssembly)));
        Assert.False(string.IsNullOrWhiteSpace(Header(headers, TransportHeaders.HostBusVersion)));
        Assert.False(string.IsNullOrWhiteSpace(Header(headers, TransportHeaders.HostFrameworkVersion)));
    }

    [Fact]
    public async Task Produce_AnEnvelopeStampedByAnotherHost_ReStampsTheHost()
    {
        Dictionary<string, string> headers = new Dictionary<string, string>()
        {
            [TransportHeaders.HostMachineName] = "another-host"
        };

        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), headers, TestContext.Current.CancellationToken);

        Assert.Equal(
            Environment.MachineName,
            Header(Assert.Single(_channel.Published).Headers!, TransportHeaders.HostMachineName));
    }

    [Fact]
    public async Task Produce_AnEnvelopeWithAMessageId_CarriesItAsAHeader()
    {
        Guid messageId = Guid.NewGuid();
        Dictionary<string, string> headers = new Dictionary<string, string>()
        {
            [TransportHeaders.MessageId] = messageId.ToString()
        };

        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), headers, TestContext.Current.CancellationToken);

        Assert.Equal(
            messageId.ToString(),
            (string)Assert.Single(_channel.Published).Headers![TransportHeaders.MessageId]!);
    }

    [Fact]
    public async Task Produce_AnEnvelopeWithIdsTypeAndTime_MapsThemToNativeAmqpProperties()
    {
        Guid messageId = Guid.NewGuid();
        Guid conversationId = Guid.NewGuid();
        DateTime occurredAt = new DateTime(2026, 7, 7, 12, 30, 45, DateTimeKind.Utc);

        Dictionary<string, string> headers = new Dictionary<string, string>()
        {
            [TransportHeaders.MessageId] = messageId.ToString(),
            [TransportHeaders.ConversationId] = conversationId.ToString(),
            [TransportHeaders.MessageType] = "Orders.PlaceOrder",
            [TransportHeaders.MessageOccurredAt] = occurredAt.ToString("O")
        };

        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), headers, TestContext.Current.CancellationToken);

        ChannelFake.Publish publish = Assert.Single(_channel.Published);
        Assert.Equal(messageId.ToString(), publish.MessageId);
        Assert.Equal(conversationId.ToString(), publish.CorrelationId);
        Assert.Equal("Orders.PlaceOrder", publish.Type);
        Assert.Equal("application/json", publish.ContentType);
        Assert.Equal(new DateTimeOffset(occurredAt).ToUnixTimeSeconds(), publish.Timestamp);
        Assert.False(string.IsNullOrWhiteSpace(publish.AppId)); // the host assembly, stamped by the producer
    }

    [Fact]
    public async Task Produce_NonGuidMessageId_LeavesTheNativeMessageIdUnset()
    {
        // the native mirroring is best-effort: an undecodable jcm-message-id leaves BasicProperties
        // .MessageId unset rather than stamping garbage. The valid-guid case is covered by
        // Produce_AnEnvelopeWithIdsTypeAndTime_MapsThemToNativeAmqpProperties.
        Dictionary<string, string> headers = new Dictionary<string, string>()
        {
            [TransportHeaders.MessageId] = "not-a-guid"
        };

        await Sut().Produce("orders", string.Empty, "{}"u8.ToArray(), headers, TestContext.Current.CancellationToken);

        Assert.Null(Assert.Single(_channel.Published).MessageId);
    }

    [Fact]
    public async Task Produce_ConcurrentAcrossExchanges_OpensExactlyOneChannelPerExchange()
    {
        // the singleton's per-exchange channel map + gate under real contention: many concurrent
        // produces spread across several exchanges (several per exchange) open exactly one channel
        // per distinct exchange and never double-open. Outcome asserted after WhenAll, not timing.
        RabbitProducer sut = Sut();
        string[] exchanges = new string[] { "orders", "orders.created", "billing", "shipping" };

        Task[] produces = Enumerable.Range(0, 200).Select(i =>
            Task.Run(
                () => sut.Produce(
                    exchanges[i % exchanges.Length],
                    string.Empty,
                    "{}"u8.ToArray(),
                    Headers(),
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken)).ToArray();

        await Task.WhenAll(produces);

        Assert.Equal(exchanges.Length, _connection.Created);
    }

    // the client's header table is object?-typed at the AMQP edge, but the producer copies the
    // envelope's canonical text straight in, so each value arrives as a string (encoded longstr).
    private static string? Header(
        IReadOnlyDictionary<string, object?> headers,
        string key
    ) => headers.TryGetValue(key, out object? value) && value is string text ? text : null;
}
