using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Domain.Commands;

public sealed class CommandContextTests
{
    private static readonly Guid _messageId = Guid.NewGuid();
    private static readonly Guid _conversationId = Guid.NewGuid();
    private static readonly Guid _aggregateId = Guid.NewGuid();
    private static readonly Guid _aggregateCorrelationId = Guid.NewGuid();
    private static readonly DateTime _occurredAt = new DateTime(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

    private static Transport Transport()
    {
        Dictionary<string, object?> headers = new Dictionary<string, object?>()
        {
            [TransportHeaders.MessageId] = TransportHeaders.ToHeader(_messageId),
            [TransportHeaders.MessageType] = TransportHeaders.ToHeader("PlaceOrder"),
            [TransportHeaders.MessageDestinationAddress] = TransportHeaders.ToHeader("orders"),
            [TransportHeaders.MessageOriginAddress] = TransportHeaders.ToHeader("orders.origin"),
            [TransportHeaders.MessageOccurredAt] = TransportHeaders.ToHeader(_occurredAt.ToString("O")),
            [TransportHeaders.ConversationId] = TransportHeaders.ToHeader(_conversationId),
            [TransportHeaders.ConversationAddress] = TransportHeaders.ToHeader("orders"),
            [TransportHeaders.ConversationOccurredAt] = TransportHeaders.ToHeader(_occurredAt.ToString("O")),
            [TransportHeaders.AggregateId] = TransportHeaders.ToHeader(_aggregateId),
            [TransportHeaders.AggregateCorrelationId] = TransportHeaders.ToHeader(_aggregateCorrelationId),
            [TransportHeaders.AggregateOccurredAt] = TransportHeaders.ToHeader(_occurredAt.ToString("O")),
            [TransportHeaders.RetryCount] = TransportHeaders.ToHeader(2),
            [TransportHeaders.HostMachineName] = TransportHeaders.ToHeader("box-1"),
            [TransportHeaders.HostAssembly] = TransportHeaders.ToHeader("MyApp"),
            [TransportHeaders.HostAssemblyVersion] = TransportHeaders.ToHeader("1.2.3.0"),
            [TransportHeaders.HostFrameworkVersion] = TransportHeaders.ToHeader("10.0.8"),
            [TransportHeaders.HostBusVersion] = TransportHeaders.ToHeader("2.0.0.0"),
            [TransportHeaders.HostOperatingSystemVersion] = TransportHeaders.ToHeader("Unix 6.8")
        };

        return new Transport(
            headers,
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);
    }

    private static CommandContext<TestCommand> CreateSut() => new CommandContext<TestCommand>(
        new TestCommand("pepe"),
        Transport());

    [Fact]
    public void MessageAndTransport_AreTheDeliveredPair()
    {
        TestCommand command = new TestCommand("pepe");
        Transport transport = Transport();

        CommandContext<TestCommand> context = new CommandContext<TestCommand>(
            command,
            transport);

        Assert.Same(command, context.Message);
        Assert.Same(transport, context.Transport);
    }

    [Fact]
    public void AggregateTrace_ReadsFromTheTransportHeaders()
    {
        CommandContext<TestCommand> context = CreateSut();

        Assert.Equal(_aggregateId, context.AggregateId);
        Assert.Equal(_aggregateCorrelationId, context.AggregateCorrelationId);
        Assert.Equal(_occurredAt, context.AggregateOccurredAt);
    }

    [Fact]
    public void Conversation_ReadsFromTheTransportHeaders()
    {
        CommandContext<TestCommand> context = CreateSut();

        Assert.Equal(_conversationId, context.ConversationId);
        Assert.Equal("orders", context.ConversationAddress);
        Assert.Equal(_occurredAt, context.ConversationOccurredAt);
    }

    [Fact]
    public void RetryCount_ReadsFromTheTransportHeaders()
        => Assert.Equal(2, CreateSut().RetryCount);

    [Fact]
    public void Host_ReadsFromTheTransportHeaders()
    {
        CommandContext<TestCommand> context = CreateSut();

        Assert.Equal("box-1", context.HostMachineName);
        Assert.Equal("MyApp", context.HostAssembly);
        Assert.Equal("1.2.3.0", context.HostAssemblyVersion);
        Assert.Equal("10.0.8", context.HostFrameworkVersion);
        Assert.Equal("2.0.0.0", context.HostBusVersion);
        Assert.Equal("Unix 6.8", context.HostOperatingSystemVersion);
    }

    [Fact]
    public void MessageFacet_ReadsFromTheTransportHeaders()
    {
        // the message facet is the envelope's own identity, separate from the aggregate's and the
        // conversation's: it is what identifies THIS delivery, so a handler logging or deduplicating on
        // it depends on every one of these reading through to the transport.
        CommandContext<TestCommand> context = CreateSut();

        Assert.Equal(_messageId, context.MessageId);
        Assert.Equal("PlaceOrder", context.MessageType);
        Assert.Equal("orders", context.MessageDestinationAddress);
        Assert.Equal("orders.origin", context.MessageOriginAddress);
        Assert.Equal(_occurredAt, context.MessageOccurredAt);
    }
}
