using System.Globalization;
using System.Text;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitTransport = JorgeCostaMacia.Bus.RabbitMQ.Domain.Transport;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>
/// Delivery, transport and header builders shared across the consumer tests — one place so the error,
/// fault and bus tests all drive the failure policy off the same envelope shape. The transport is built
/// from a real <see cref="BasicDeliverEventArgs"/> (as the worker does), so the tests exercise the same
/// header-decoding path the runtime does.
/// </summary>
internal static class Deliveries
{
    /// <summary>The exchange the tests publish and consume on.</summary>
    public const string Exchange = "orders";

    /// <summary>The queue the tests consume from — the failing "group" stamped on parked failures.</summary>
    public const string Queue = "orders.handler";

    /// <summary>A transport over a minimal envelope (retry count + aggregate trace) — for the error/fault handler tests
    /// that only need the transport.</summary>
    public static Transport Transport(
        int retryCount = 0,
        Guid? aggregateId = null,
        Guid? aggregateCorrelationId = null
    ) => RabbitTransport.Create(Args("{}"u8.ToArray(), TraceHeaders(retryCount, aggregateId, aggregateCorrelationId)));

    /// <summary>A well-formed delivery carrying the serialized message and the aggregate trace;
    /// <paramref name="consumers"/> stamps the <c>AggregateConsumers</c> header.</summary>
    public static BasicDeliverEventArgs Delivery<TMessage>(
        TMessage message,
        ulong deliveryTag = 10,
        string? consumers = null
    )
    {
        Dictionary<string, object?> headers = TraceHeaders();

        if (consumers is not null)
        {
            headers[TransportHeaders.AggregateConsumers] = Encoding.UTF8.GetBytes(consumers);
        }

        return Args(JsonSerializer.SerializeToUtf8Bytes(message), headers, deliveryTag);
    }

    /// <summary>A delivery whose body is not valid JSON — drives the malformed/fault path.</summary>
    public static BasicDeliverEventArgs Garbage(ulong deliveryTag = 10)
        => Args("}{ not json"u8.ToArray(), new Dictionary<string, object?>(), deliveryTag);

    /// <summary>A delivery whose body deserializes to <see langword="null"/> — drives the null-body/fault
    /// path.</summary>
    public static BasicDeliverEventArgs NullBody(ulong deliveryTag = 10)
        => Args("null"u8.ToArray(), new Dictionary<string, object?>(), deliveryTag);

    /// <summary>Reads a published message's canonical header text, or <see langword="null"/> when absent.</summary>
    public static string? Header(
        IReadOnlyDictionary<string, string> headers,
        string key
    ) => headers.TryGetValue(key, out string? value) ? value : null;

    /// <summary>Builds the delivery args from a body, its headers and a delivery tag — the AMQP shape the worker
    /// receives.</summary>
    public static BasicDeliverEventArgs Args(
        byte[] body,
        Dictionary<string, object?> headers,
        ulong deliveryTag = 10
    )
        => new BasicDeliverEventArgs(
            consumerTag: "consumer-tag",
            deliveryTag: deliveryTag,
            redelivered: false,
            exchange: Exchange,
            routingKey: string.Empty,
            properties: new BasicProperties { Headers = headers! },
            body: body);

    // the incoming AMQP field table hands values back as bytes (the client decodes a longstr to byte[]),
    // so the envelope's canonical text arrives as its UTF-8 bytes — GUIDs included, as dashed text now.
    private static Dictionary<string, object?> TraceHeaders(
        int retryCount = 0,
        Guid? aggregateId = null,
        Guid? aggregateCorrelationId = null
    )
        => new Dictionary<string, object?>()
        {
            [TransportHeaders.RetryCount] = Encoding.UTF8.GetBytes(retryCount.ToString(CultureInfo.InvariantCulture)),
            [TransportHeaders.AggregateId] = Encoding.UTF8.GetBytes((aggregateId ?? Guid.NewGuid()).ToString()),
            [TransportHeaders.AggregateCorrelationId] = Encoding.UTF8.GetBytes(
                (aggregateCorrelationId ?? Guid.NewGuid()).ToString())
        };
}
