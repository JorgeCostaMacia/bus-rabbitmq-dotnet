using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using FaultHandler = JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events.EventFaultHandler<
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.TestEvent,
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.TestEventSubscriber>;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure.Consumers.Events;

public sealed class EventFaultHandlerTests
{
    private readonly ProducerFake _producer = new ProducerFake();

    private FaultHandler Fault(ILogger? logger = null)
        => new FaultHandler(_producer, logger ?? NullLogger.Instance, Deliveries.Queue);

    [Fact]
    public async Task Handle_AnUnparseableBody_ParksItToTheFaultQueueAsText()
    {
        EventFaultContext context = EventFaultContext.Create(
            "not json"u8.ToArray(),
            Deliveries.Transport(),
            new InvalidCastException("bad header"));

        FaultHandler sut = Fault();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Parked, sut.Result);
        (string exchange, string routingKey, byte[] body, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.Queue}.fault", routingKey);

        JsonElement parked = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal(
            typeof(InvalidCastException).FullName,
            parked.GetProperty("error").GetProperty("type").GetString());
        Assert.Equal("bad header", parked.GetProperty("error").GetProperty("message").GetString());
        Assert.Equal(Deliveries.Queue, parked.GetProperty("queue").GetString());
        Assert.Equal("not json", parked.GetProperty("message").GetString());
    }

    [Fact]
    public async Task Handle_ANestedExceptionWithData_ParksTheFullChainAndTheHost()
    {
        Exception failure = new InvalidCastException(
            "outer",
            new FormatException("the real cause"));
        failure.Data["field"] = "required";
        EventFaultContext context = EventFaultContext.Create(
            "not json"u8.ToArray(),
            Deliveries.Transport(),
            failure);

        await Fault().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Body);
        JsonElement error = body.GetProperty("error");
        Assert.Equal("the real cause", error.GetProperty("innerError").GetProperty("message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("innerError").GetProperty("type").GetString());
        Assert.Equal("required", error.GetProperty("data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("machineName").GetString());
    }

    [Fact]
    public async Task Handle_AnUnparseableBody_StampsTheFailureOnTheParkedHeaders()
    {
        EventFaultContext context = EventFaultContext.Create(
            "not json"u8.ToArray(),
            Deliveries.Transport(),
            new InvalidCastException("bad header"));

        await Fault().Handle(context, TestContext.Current.CancellationToken);

        IReadOnlyDictionary<string, string> headers = Assert.Single(_producer.Produced).Headers;
        Assert.Equal(typeof(InvalidCastException).FullName, Deliveries.Header(headers, TransportHeaders.ErrorType));
        Assert.Equal("bad header", Deliveries.Header(headers, TransportHeaders.ErrorMessage));
        Assert.Equal(Deliveries.Queue, Deliveries.Header(headers, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task Handle_ProduceFails_LeavesUnhandled()
    {
        _producer.Failure = new InvalidOperationException("broker down");

        FaultHandler sut = Fault();
        await sut.Handle(
            EventFaultContext.Create(
                "{}"u8.ToArray(),
                Deliveries.Transport(),
                new InvalidCastException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task Handle_ShutdownWhileParking_LeavesUnhandledSilently()
    {
        // a deploy cancels the token while the fault is being parked. The outcome matches a broker
        // failure — Unhandled, so the delivery is redelivered rather than acked away — but it must get
        // there silently: the generic catch logs "ParkingFailed" at Error, and a routine shutdown
        // must not look like a parking outage.
        RecordingLogger<EventFaultHandlerTests> logger = new RecordingLogger<EventFaultHandlerTests>();
        _producer.Failure = new OperationCanceledException();
        FaultHandler sut = Fault(logger);

        await sut.Handle(
            EventFaultContext.Create(
                "not json"u8.ToArray(),
                Deliveries.Transport(),
                new InvalidCastException("bad header")),
            TestContext.Current.CancellationToken);

        Assert.Equal(FaultResult.Unhandled, sut.Result);
        Assert.Empty(_producer.Produced);
        Assert.DoesNotContain(logger.Logged, entry => entry.Level >= LogLevel.Error);
    }
}
