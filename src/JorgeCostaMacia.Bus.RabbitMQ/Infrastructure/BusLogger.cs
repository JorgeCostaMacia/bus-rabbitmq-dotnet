using System.Globalization;
using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;
using Serilog.Context;
using Serilog.Core;
using Serilog.Core.Enrichers;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The bus's logging: the contexts (the worker's identity, and the inbound delivery — body and
/// envelope decoded, inspectable and reinjectable from the log platform) and the retry warnings.
/// Every outcome log carries the minimal, groupable template; the variable data travels through
/// Serilog's <see cref="LogContext"/> — the accepted core coupling, the host enriches
/// <c>FromLogContext</c> — with the failures expanded by a <c>BusDescription</c> from
/// <see cref="BusLoggerDescriptions"/>, so they are indexable and manageable from the log platform.
/// </summary>
internal static class BusLogger
{
    /// <summary>
    /// Opens the context stamping the outcome's <c>BusDescription</c> (one of
    /// <see cref="BusLoggerDescriptions"/>) on the log written inside it.
    /// </summary>
    /// <param name="description">The expansion, from <see cref="BusLoggerDescriptions"/>.</param>
    /// <returns>The context to dispose after logging the outcome.</returns>
    public static IDisposable DescriptionContext(string description)
        => LogContext.PushProperty("BusDescription", description);

    /// <summary>
    /// Opens the logging context carrying the consumer worker's identity — the exchange it binds to
    /// and the queue it consumes — for the whole delivery.
    /// </summary>
    /// <param name="exchange">The exchange the worker's queue binds to.</param>
    /// <param name="queue">The queue the worker consumes.</param>
    /// <returns>The context to dispose when the delivery ends.</returns>
    public static IDisposable WorkerContext(
        string exchange,
        string queue
    ) => LogContext.Push(new PropertyEnricher("Exchange", exchange), new PropertyEnricher("Queue", queue));

    /// <summary>
    /// Opens the logging context carrying the whole inbound delivery — the routing key, delivery tag
    /// and redelivered flag, the raw body and every envelope header decoded to its type — so every
    /// log inside it (the handler's own included, and the failure lanes) is fully traced and a failed
    /// message can be inspected from the log platform.
    /// </summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>The context to dispose when the delivery's iteration ends.</returns>
    public static IDisposable ConsumerContext(BasicDeliverEventArgs args)
    {
        List<ILogEventEnricher> context = new List<ILogEventEnricher>
        {
            new PropertyEnricher("RoutingKey", args.RoutingKey),
            new PropertyEnricher("DeliveryTag", args.DeliveryTag),
            new PropertyEnricher("Redelivered", args.Redelivered),
            new PropertyEnricher("Body", args.Body.Length == 0 ? null : Encoding.UTF8.GetString(args.Body.Span))
        };

        Decode(context, args.BasicProperties.Headers);

        return LogContext.Push(context.ToArray());
    }

    /// <summary>
    /// Opens the logging context carrying the whole OUTBOUND delivery — the destination exchange and
    /// routing key, the raw body and every envelope header typed — so a failed produce is inspectable
    /// and reinjectable from the log platform, the send side's mirror of <see cref="ConsumerContext"/>.
    /// </summary>
    /// <param name="exchange">The destination exchange (empty for a park through the default exchange).</param>
    /// <param name="routingKey">The routing key (the queue name for a park).</param>
    /// <param name="body">The serialized message body.</param>
    /// <param name="headers">The outbound envelope headers.</param>
    /// <returns>The context to dispose after logging the failure.</returns>
    public static IDisposable ProducerContext(
        string exchange,
        string routingKey,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers
    )
    {
        List<ILogEventEnricher> context = new List<ILogEventEnricher>
        {
            new PropertyEnricher("Exchange", exchange),
            new PropertyEnricher("RoutingKey", routingKey),
            new PropertyEnricher("Body", body.Length == 0 ? null : Encoding.UTF8.GetString(body.Span))
        };

        foreach ((string key, string value) in headers)
        {
            context.Add(new PropertyEnricher(
                key,
                TransportHeaders.GuidHeaders.Contains(key) && Guid.TryParse(value, out Guid id)
                    ? id
                    : TransportHeaders.IntHeaders.Contains(key)
                        && int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int count)
                        ? count
                        : value));
        }

        return LogContext.Push(context.ToArray());
    }

    /// <summary>
    /// Opens the logging context carrying the channel's shutdown reason — the broker's reply code and
    /// text when the close carried them.
    /// </summary>
    /// <param name="reason">The shutdown reason, or <see langword="null"/> when the broker cancelled the consumer
    /// without one.</param>
    /// <returns>The context to dispose after logging the outcome.</returns>
    public static IDisposable ShutdownContext(ShutdownEventArgs? reason)
        => LogContext.Push(
            new PropertyEnricher("ReplyCode", reason?.ReplyCode),
            new PropertyEnricher("ReplyText", reason?.ReplyText));

    /// <summary>Logs a failed handling republished to retry, with the retry number in the context.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="retry">The retry number stamped on the republished delivery.</param>
    public static void LogRetry(
        ILogger logger,
        Exception exception,
        int retry
    )
    {
        using (LogContext.Push(
            new PropertyEnricher("Retry", retry),
            new PropertyEnricher("BusDescription", BusLoggerDescriptions.RepublishedToRetry)))
        {
            logger.LogWarning(exception, "Handler failed.");
        }
    }

    /// <summary>Logs a failed handling parked for a delayed retry, with the retry number and its time in the
    /// context.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The handling failure.</param>
    /// <param name="retry">The retry number stamped on the parked delivery.</param>
    /// <param name="scheduledAt">The UTC time the retry is produced back at.</param>
    public static void LogRetry(
        ILogger logger,
        Exception exception,
        int retry,
        DateTime scheduledAt
    )
    {
        using (LogContext.Push(
            new PropertyEnricher("Retry", retry),
            new PropertyEnricher("ScheduledAt", scheduledAt),
            new PropertyEnricher("BusDescription", BusLoggerDescriptions.ScheduledToRetry)))
        {
            logger.LogWarning(exception, "Handler failed.");
        }
    }

    /// <summary>Decodes every envelope header into the context — Guids and counters typed, the rest as text. A delivery
    /// with no headers contributes nothing rather than throwing — logging must never fault, least of all in a catch
    /// about to rethrow.</summary>
    /// <param name="context">The enricher list to fill.</param>
    /// <param name="headers">The delivery's headers, or <see langword="null"/> when the message carries none.</param>
    private static void Decode(
        List<ILogEventEnricher> context,
        IDictionary<string, object?>? headers
    )
    {
        if (headers is null)
        {
            return;
        }

        foreach ((string key, object? raw) in headers)
        {
            if (raw is not byte[] value)
            {
                context.Add(new PropertyEnricher(key, raw));

                continue;
            }

            string text = Encoding.UTF8.GetString(value);

            context.Add(new PropertyEnricher(
                key,
                TransportHeaders.GuidHeaders.Contains(key) && Guid.TryParse(text, out Guid id)
                    ? id
                    : TransportHeaders.IntHeaders.Contains(key)
                        && int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int count)
                        ? count
                        : text));
        }
    }
}
