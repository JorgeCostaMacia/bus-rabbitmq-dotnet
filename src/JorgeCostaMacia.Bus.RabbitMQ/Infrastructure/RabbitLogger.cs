using Microsoft.Extensions.Logging;
using Serilog.Context;
using Serilog.Core.Enrichers;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The RabbitMQ client's logging — the connection-level callbacks (shutdown, automatic-recovery
/// success and failure, and callback exceptions), routed to the logger with the details as
/// <see cref="LogContext"/> properties. Everything logs under the dedicated <see cref="Category"/>,
/// separate from the bus's own categories, so the client's noise is silenced independently. The
/// mirror of the Kafka transport's client logger.
/// </summary>
internal static class RabbitLogger
{
    /// <summary>
    /// The logger category every client callback logs under (e.g. silence it with
    /// <c>Logging:LogLevel:JorgeCostaMacia.Bus.RabbitMQ.Client = Warning</c>).
    /// </summary>
    public const string Category = "JorgeCostaMacia.Bus.RabbitMQ.Client";

    /// <summary>Logs a connection shutdown — informational when the application closed it, a warning when the peer or
    /// the library dropped it (automatic recovery then kicks in).</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="replyCode">The AMQP reply code of the shutdown.</param>
    /// <param name="replyText">The AMQP reply text of the shutdown.</param>
    /// <param name="applicationInitiated">Whether the application initiated the shutdown (an orderly close) rather than
    /// the peer/library (a drop).</param>
    public static void LogShutdown(
        ILogger logger,
        ushort replyCode,
        string replyText,
        bool applicationInitiated
    )
    {
        using (LogContext.Push(
            new PropertyEnricher("ReplyCode", replyCode),
            new PropertyEnricher("ReplyText", replyText)))
        {
            logger.Log(applicationInitiated ? LogLevel.Information : LogLevel.Warning, "Connection shut down.");
        }
    }

    /// <summary>Logs a successful automatic recovery — the connection (and its topology) is back after a
    /// drop.</summary>
    /// <param name="logger">The logger.</param>
    public static void LogRecovered(ILogger logger) => logger.LogInformation("Connection recovered.");

    /// <summary>Logs a failed automatic-recovery attempt — the client keeps retrying on its own.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The recovery failure.</param>
    public static void LogRecoveryError(
        ILogger logger,
        Exception exception
    ) => logger.LogWarning(exception, "Connection recovery failed.");

    /// <summary>Logs an exception thrown by a client callback — a bug in a handler the client invoked, swallowed by the
    /// client otherwise.</summary>
    /// <param name="logger">The logger.</param>
    /// <param name="exception">The callback failure.</param>
    public static void LogCallbackException(
        ILogger logger,
        Exception exception
    ) => logger.LogError(exception, "Client callback failed.");
}
