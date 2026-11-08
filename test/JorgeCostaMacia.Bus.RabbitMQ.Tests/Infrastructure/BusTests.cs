using System.Text;
using System.Text.Json;
using JorgeCostaMacia.Bus.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using RabbitBus = JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Bus;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure;

public sealed class BusTests
{
    private sealed record ForeignTransport : ITransport;

    private readonly ProducerFake _producer = new ProducerFake();

    private RabbitBus CreateSut(params (Type Type, string Exchange)[] messages)
        => new RabbitBus(_producer, messages.ToDictionary(e => e.Type, e => e.Exchange));

    private static string? Header(
        IReadOnlyDictionary<string, string> headers,
        string key
    ) => headers.TryGetValue(key, out string? value) ? value : null;

    private static Guid GuidHeader(
        IReadOnlyDictionary<string, string> headers,
        string key
    )
    {
        Assert.True(headers.TryGetValue(key, out string? value) && value is not null);

        return Guid.Parse(value!);
    }

    [Fact]
    public async Task Send_UnmappedType_Throws()
    {
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateSut().Send(new TestCommand("pepe"), TestContext.Current.CancellationToken));

        Assert.Contains(typeof(TestCommand).FullName!, exception.Message);
    }

    [Fact]
    public async Task Send_WithoutTransport_BuildsANewEnvelope()
    {
        TestCommand command = new TestCommand(
            "pepe",
            aggregateConsumers: new string[] { "g1", "g2" });

        await CreateSut((typeof(TestCommand), "orders")).Send(command, TestContext.Current.CancellationToken);

        (string exchange, string routingKey, byte[] body, IReadOnlyDictionary<string, string> headers) = Assert.Single(
            _producer.Produced);
        Assert.Equal("orders", exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal(typeof(TestCommand).FullName, Header(headers, TransportHeaders.MessageType));
        Assert.Equal("orders", Header(headers, TransportHeaders.MessageDestinationAddress));
        Assert.Null(Header(headers, TransportHeaders.MessageOriginAddress));
        Assert.Equal("orders", Header(headers, TransportHeaders.ConversationAddress));
        Assert.Equal(
            GuidHeader(headers, TransportHeaders.MessageId),
            GuidHeader(headers, TransportHeaders.ConversationId));
        Assert.Equal(command.AggregateId, GuidHeader(headers, TransportHeaders.AggregateId));
        Assert.Equal(command.AggregateCorrelationId, GuidHeader(headers, TransportHeaders.AggregateCorrelationId));
        Assert.Equal("g1,g2", Header(headers, TransportHeaders.AggregateConsumers));
        Assert.Equal("0", Header(headers, TransportHeaders.RetryCount));
        Assert.Contains("pepe", Encoding.UTF8.GetString(body));
    }

    [Fact]
    public async Task Publish_WithTransport_ContinuesTheConversation()
    {
        Guid inboundMessageId = Guid.NewGuid();
        Guid conversationId = Guid.NewGuid();

        Dictionary<string, object?> inbound = new Dictionary<string, object?>()
        {
            [TransportHeaders.MessageId] = Encoding.UTF8.GetBytes(inboundMessageId.ToString()),
            [TransportHeaders.MessageType] = "Inbound"u8.ToArray(),
            [TransportHeaders.MessageDestinationAddress] = "orders"u8.ToArray(),
            [TransportHeaders.MessageOccurredAt] = Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
            [TransportHeaders.ConversationId] = Encoding.UTF8.GetBytes(conversationId.ToString()),
            [TransportHeaders.ConversationAddress] = "orders"u8.ToArray(),
            [TransportHeaders.ConversationOccurredAt] = Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
            [TransportHeaders.AggregateId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.AggregateCorrelationId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.AggregateOccurredAt] = Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
            [TransportHeaders.AggregateConsumers] = "old"u8.ToArray(),
            [TransportHeaders.RetryCount] = "2"u8.ToArray()
        };

        Transport transport = new Transport(
            inbound,
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);
        TestEvent message = new TestEvent(
            "pepe",
            aggregateConsumers: new string[] { "g1" });

        await CreateSut((typeof(TestEvent), "payments")).Publish(
            message,
            transport,
            TestContext.Current.CancellationToken);

        (string exchange, _, _, IReadOnlyDictionary<string, string> headers) = Assert.Single(_producer.Produced);
        Assert.Equal("payments", exchange);
        Assert.Equal(conversationId, GuidHeader(headers, TransportHeaders.ConversationId));
        Assert.Equal("orders", Header(headers, TransportHeaders.ConversationAddress));
        Assert.Equal("orders", Header(headers, TransportHeaders.MessageOriginAddress));
        Assert.Equal("payments", Header(headers, TransportHeaders.MessageDestinationAddress));
        Assert.Equal("0", Header(headers, TransportHeaders.RetryCount));
        Assert.NotEqual(inboundMessageId, GuidHeader(headers, TransportHeaders.MessageId));
        Assert.Equal(typeof(TestEvent).FullName, Header(headers, TransportHeaders.MessageType));
        Assert.Equal(message.AggregateId, GuidHeader(headers, TransportHeaders.AggregateId));
        Assert.Equal("g1", Header(headers, TransportHeaders.AggregateConsumers));
    }

    [Fact]
    public async Task Publish_WithTransportAndAnUntargetedEvent_ClearsTheInboundTargeting()
    {
        // the continuation re-stamps AggregateConsumers from the outbound message: an untargeted
        // message (empty list) clears the inbound targeting instead of carrying it over.
        Dictionary<string, object?> inbound = new Dictionary<string, object?>()
        {
            [TransportHeaders.MessageDestinationAddress] = "orders"u8.ToArray(),
            [TransportHeaders.ConversationId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.AggregateConsumers] = "old"u8.ToArray(),
            [TransportHeaders.RetryCount] = "2"u8.ToArray()
        };

        Transport transport = new Transport(
            inbound,
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);
        TestEvent message = new TestEvent("pepe");

        await CreateSut((typeof(TestEvent), "payments")).Publish(
            message,
            transport,
            TestContext.Current.CancellationToken);

        Assert.Equal(
            string.Empty,
            Header(Assert.Single(_producer.Produced).Headers, TransportHeaders.AggregateConsumers));
    }

    [Fact]
    public async Task Publish_WithForeignTransport_ThrowsReadably()
    {
        // continuing over a transport from another bus is a wiring mistake: the failure names the
        // foreign type and the transport this bus needs, instead of an opaque cast error.
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateSut((typeof(TestEvent), "payments")).Publish(
                new TestEvent("pepe"),
                new ForeignTransport(),
                TestContext.Current.CancellationToken));

        Assert.Contains(typeof(ForeignTransport).FullName!, exception.Message);
        Assert.Contains("RabbitMQ", exception.Message);
    }

    [Fact]
    public async Task Send_ProduceFails_Rethrows()
    {
        _producer.Failure = new InvalidOperationException("broker down");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateSut((typeof(TestCommand), "orders")).Send(
                new TestCommand("pepe"),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Send_ACommandWithItsOwnFields_SerializesTheRuntimeType()
    {
        await CreateSut((typeof(TestCommand), "orders")).Send(
            new TestCommand("pepe"),
            TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Body);

        Assert.Equal("pepe", body.GetProperty("name").GetString());
    }

    [Fact]
    public async Task Publish_Batch_ProducesEveryEventInOrder()
    {
        TestEvent[] events = new TestEvent[] { new TestEvent("a"), new TestEvent("b"), new TestEvent("c") };

        await CreateSut((typeof(TestEvent), "orders.created")).Publish(events, TestContext.Current.CancellationToken);

        Assert.Equal(3, _producer.Produced.Count);
        Assert.All(_producer.Produced, produced => Assert.Equal("orders.created", produced.Exchange));
        Assert.Equal(
            new string[] { "a", "b", "c" },
            _producer.Produced.Select(
                produced => JsonSerializer.Deserialize<JsonElement>(produced.Body).GetProperty("name").GetString()));
    }

    [Fact]
    public async Task Send_Batch_ProducesEveryCommand()
    {
        TestCommand[] commands = new TestCommand[] { new TestCommand("x"), new TestCommand("y") };

        await CreateSut((typeof(TestCommand), "orders")).Send(commands, TestContext.Current.CancellationToken);

        Assert.Equal(2, _producer.Produced.Count);
        Assert.All(_producer.Produced, produced => Assert.Equal("orders", produced.Exchange));
    }

    [Fact]
    public async Task Send_BatchWithTransport_ContinuesTheConversationForEach()
    {
        Guid conversationId = Guid.NewGuid();

        Dictionary<string, object?> inbound = new Dictionary<string, object?>()
        {
            [TransportHeaders.MessageId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.MessageDestinationAddress] = "orders"u8.ToArray(),
            [TransportHeaders.ConversationId] = Encoding.UTF8.GetBytes(conversationId.ToString()),
            [TransportHeaders.ConversationAddress] = "orders"u8.ToArray(),
            [TransportHeaders.ConversationOccurredAt] = Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
            [TransportHeaders.AggregateId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.AggregateCorrelationId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.AggregateOccurredAt] = Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
            [TransportHeaders.AggregateConsumers] = "old"u8.ToArray(),
            [TransportHeaders.RetryCount] = "3"u8.ToArray()
        };

        Transport transport = new Transport(
            inbound,
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);
        TestCommand[] commands = new TestCommand[] { new TestCommand("x"), new TestCommand("y") };

        await CreateSut((typeof(TestCommand), "payments")).Send(
            commands,
            transport,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, _producer.Produced.Count);
        Assert.All(_producer.Produced, produced =>
        {
            Assert.Equal("payments", produced.Exchange);
            Assert.Equal(conversationId, GuidHeader(produced.Headers, TransportHeaders.ConversationId));
            Assert.Equal("orders", Header(produced.Headers, TransportHeaders.MessageOriginAddress));
            Assert.Equal("payments", Header(produced.Headers, TransportHeaders.MessageDestinationAddress));
            Assert.Equal("0", Header(produced.Headers, TransportHeaders.RetryCount));
        });

        Assert.Equal(
            2,
            _producer.Produced
                .Select(produced => GuidHeader(produced.Headers, TransportHeaders.MessageId))
                .Distinct()
                .Count());
    }

    [Fact]
    public async Task Publish_EmptyBatch_ProducesNothing()
    {
        await CreateSut((typeof(TestEvent), "orders.created")).Publish(
            Array.Empty<TestEvent>(),
            TestContext.Current.CancellationToken);

        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Publish_BatchWithTransport_ContinuesTheConversationForEach()
    {
        // the shape a handler actually uses: it received a command and publishes several events in
        // reaction, all continuing the same conversation. Send's twin was pinned and this one was not,
        // which left the most common fan-out in these services untested.
        Guid conversationId = Guid.NewGuid();

        Dictionary<string, object?> inbound = new Dictionary<string, object?>()
        {
            [TransportHeaders.MessageId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.MessageDestinationAddress] = "orders"u8.ToArray(),
            [TransportHeaders.ConversationId] = Encoding.UTF8.GetBytes(conversationId.ToString()),
            [TransportHeaders.ConversationAddress] = "orders"u8.ToArray(),
            [TransportHeaders.ConversationOccurredAt] = Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
            [TransportHeaders.AggregateId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.AggregateCorrelationId] = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()),
            [TransportHeaders.AggregateOccurredAt] = Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")),
            [TransportHeaders.RetryCount] = "3"u8.ToArray()
        };

        Transport transport = new Transport(
            inbound,
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);
        TestEvent[] events = new TestEvent[] { new TestEvent("a"), new TestEvent("b") };

        await CreateSut((typeof(TestEvent), "orders.created")).Publish(
            events,
            transport,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, _producer.Produced.Count);
        Assert.All(_producer.Produced, produced =>
        {
            Assert.Equal("orders.created", produced.Exchange);
            Assert.Equal(conversationId, GuidHeader(produced.Headers, TransportHeaders.ConversationId));
            Assert.Equal("orders", Header(produced.Headers, TransportHeaders.MessageOriginAddress));
            Assert.Equal("orders.created", Header(produced.Headers, TransportHeaders.MessageDestinationAddress));
            Assert.Equal("0", Header(produced.Headers, TransportHeaders.RetryCount));
        });

        Assert.Equal(
            2,
            _producer.Produced
                .Select(produced => GuidHeader(produced.Headers, TransportHeaders.MessageId))
                .Distinct()
                .Count());
    }
}
