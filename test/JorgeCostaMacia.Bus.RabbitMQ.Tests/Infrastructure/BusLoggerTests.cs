using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using Serilog.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure;

/// <summary>
/// The bus's logging context, both directions. The typing of the envelope headers is the substance —
/// an aggregate id logged as a <see cref="Guid"/> and a retry count as a number are what make a failed
/// delivery findable and its retries countable in the log platform — so the properties are read back
/// through a real Serilog pipeline rather than assumed. The mirror of the Kafka transport's suite.
/// </summary>
public sealed class BusLoggerTests
{
    private readonly CapturingSink _sink = new CapturingSink();

    [Fact]
    public void ProducerContext_TypesTheEnvelopeHeaders()
    {
        Guid aggregateId = Guid.CreateVersion7();
        Dictionary<string, string> headers = new Dictionary<string, string>()
        {
            [TransportHeaders.AggregateId] = aggregateId.ToString(),
            [TransportHeaders.RetryCount] = "2",
            [TransportHeaders.MessageType] = "PlaceOrder"
        };

        LogEvent captured = Capture(
            () => BusLogger.ProducerContext("orders", "orders.handler", Body("payload"), headers));

        Assert.Equal(aggregateId, CapturingSink.Scalar(captured, TransportHeaders.AggregateId));
        Assert.Equal(2, CapturingSink.Scalar(captured, TransportHeaders.RetryCount));
        Assert.Equal("PlaceOrder", CapturingSink.Scalar(captured, TransportHeaders.MessageType));
    }

    [Fact]
    public void ProducerContext_WithAnUnparseableId_FallsBackToText()
    {
        // the parse guard is what stops a malformed envelope throwing inside the logging of a produce
        // failure — which would replace the real error with this one.
        Dictionary<string, string> headers = new Dictionary<string, string>()
        {
            [TransportHeaders.AggregateId] = "not-a-guid"
        };

        LogEvent captured = Capture(
            () => BusLogger.ProducerContext("orders", "orders.handler", Body("payload"), headers));

        Assert.Equal("not-a-guid", CapturingSink.Scalar(captured, TransportHeaders.AggregateId));
    }

    [Fact]
    public void ProducerContext_WithAnUnparseableCount_FallsBackToText()
    {
        Dictionary<string, string> headers = new Dictionary<string, string>()
        {
            [TransportHeaders.RetryCount] = "many"
        };

        LogEvent captured = Capture(
            () => BusLogger.ProducerContext("orders", "orders.handler", Body("payload"), headers));

        Assert.Equal("many", CapturingSink.Scalar(captured, TransportHeaders.RetryCount));
    }

    [Fact]
    public void ProducerContext_WithAnEmptyBody_RecordsANullBody()
    {
        // an empty body reads as absent rather than as an empty string, so a log query for a missing
        // payload finds it.
        LogEvent captured = Capture(
            () => BusLogger.ProducerContext(
                "orders",
                "orders.handler",
                ReadOnlyMemory<byte>.Empty,
                new Dictionary<string, string>()));

        Assert.Equal("orders", CapturingSink.Scalar(captured, "Exchange"));
        Assert.Null(CapturingSink.Scalar(captured, "Body"));
    }

    [Fact]
    public void ConsumerContext_TypesTheInboundHeaders_WhichArriveAsBytes()
    {
        // inbound is the asymmetric half: the AMQP field table hands every value back as byte[], so the
        // decode has to go through UTF-8 first — a path the outbound side never takes.
        Guid aggregateId = Guid.CreateVersion7();
        Dictionary<string, object?> headers = new Dictionary<string, object?>()
        {
            [TransportHeaders.AggregateId] = Encoding.UTF8.GetBytes(aggregateId.ToString()),
            [TransportHeaders.RetryCount] = Encoding.UTF8.GetBytes("4")
        };

        LogEvent captured = Capture(
            () => BusLogger.ConsumerContext(Deliveries.Args(Encoding.UTF8.GetBytes("payload"), headers)));

        Assert.Equal(aggregateId, CapturingSink.Scalar(captured, TransportHeaders.AggregateId));
        Assert.Equal(4, CapturingSink.Scalar(captured, TransportHeaders.RetryCount));
    }

    [Fact]
    public void ConsumerContext_WithAnEmptyBody_RecordsANullBody()
    {
        LogEvent captured = Capture(
            () => BusLogger.ConsumerContext(Deliveries.Args(Array.Empty<byte>(), new Dictionary<string, object?>())));

        Assert.Null(CapturingSink.Scalar(captured, "Body"));
        Assert.Equal(10UL, CapturingSink.Scalar(captured, "DeliveryTag"));
    }

    [Fact]
    public void ConsumerContext_WithNoHeaderTableAtAll_StillLogsTheDelivery()
    {
        // a delivery published without a field table arrives with Headers null, and the decode returns
        // early rather than faulting — logging must never be the thing that throws, least of all inside
        // a catch about to rethrow the real failure.
        BasicDeliverEventArgs args = new BasicDeliverEventArgs(
            consumerTag: "consumer-tag",
            deliveryTag: 7,
            redelivered: true,
            exchange: "orders",
            routingKey: string.Empty,
            properties: new BasicProperties(),
            body: Encoding.UTF8.GetBytes("payload"));

        LogEvent captured = Capture(() => BusLogger.ConsumerContext(args));

        Assert.Equal(7UL, CapturingSink.Scalar(captured, "DeliveryTag"));
        Assert.Equal(true, CapturingSink.Scalar(captured, "Redelivered"));
        Assert.Equal("payload", CapturingSink.Scalar(captured, "Body"));
    }

    /// <summary>The UTF-8 bytes of a body, as the transport carries it.</summary>
    /// <param name="body">The body text.</param>
    /// <returns>The body bytes.</returns>
    private static ReadOnlyMemory<byte> Body(string body) => Encoding.UTF8.GetBytes(body);

    /// <summary>
    /// Runs a context factory inside a real Serilog pipeline and returns the single event written while
    /// the context was open, so the pushed properties can be read back.
    /// </summary>
    /// <param name="context">The context factory under test.</param>
    /// <returns>The captured event.</returns>
    private LogEvent Capture(Func<IDisposable> context)
    {
        Log.Logger = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .WriteTo.Sink(_sink)
            .CreateLogger();

        try
        {
            using (context())
            {
                Log.Information("probe");
            }
        }
        finally
        {
            Log.CloseAndFlush();
        }

        return Assert.Single(_sink.Events);
    }
}
