using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure;

/// <summary>
/// The RabbitMQ client's callback logging, the mirror of the Kafka transport's client logger. These are
/// visibility-only behaviours — the log is the whole effect — and the level is the substance: it decides
/// whether a connection drop reads as routine or as something an operator has to look at.
/// </summary>
public sealed class RabbitLoggerTests
{
    [Fact]
    public void LogShutdown_WhenTheApplicationClosedTheConnection_IsInformational()
    {
        // an orderly close on our own side is what every shutdown looks like; warning about it would
        // put a warning in the log on every deploy.
        RecordingLogger<RabbitLoggerTests> logger = new RecordingLogger<RabbitLoggerTests>();

        RabbitLogger.LogShutdown(logger, 200, "Goodbye", applicationInitiated: true);

        (LogLevel Level, string Message) entry = Assert.Single(logger.Logged);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal("Connection shut down.", entry.Message);
    }

    [Fact]
    public void LogShutdown_WhenThePeerDroppedTheConnection_Warns()
    {
        // the peer or the library dropping it is unplanned: automatic recovery takes over, but the drop
        // itself is worth seeing — it is the start of any recovery story in the log.
        RecordingLogger<RabbitLoggerTests> logger = new RecordingLogger<RabbitLoggerTests>();

        RabbitLogger.LogShutdown(logger, 320, "CONNECTION_FORCED", applicationInitiated: false);

        Assert.Equal(LogLevel.Warning, Assert.Single(logger.Logged).Level);
    }

    [Fact]
    public void LogRecovered_ARecoveredConnection_LogsAtInformation()
    {
        RecordingLogger<RabbitLoggerTests> logger = new RecordingLogger<RabbitLoggerTests>();

        RabbitLogger.LogRecovered(logger);

        (LogLevel Level, string Message) entry = Assert.Single(logger.Logged);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.Equal("ConnectionRecovered", entry.Message);
    }

    [Fact]
    public void LogRecoveryError_AFailedRecoveryAttempt_Warns()
    {
        // a failed recovery attempt is not the end: the client retries on its own, so this must not read
        // as a dead connection.
        RecordingLogger<RabbitLoggerTests> logger = new RecordingLogger<RabbitLoggerTests>();

        RabbitLogger.LogRecoveryError(logger, new InvalidOperationException("no route to host"));

        (LogLevel Level, string Message) entry = Assert.Single(logger.Logged);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Equal("ConnectionRecoveryFailed", entry.Message);
    }

    [Fact]
    public void LogCallbackException_AThrowingClientCallback_LogsAtError()
    {
        // this one is a bug in a handler the client invoked, and the client swallows it — without this
        // log it leaves no trace at all, which is why it is Error and not Warning.
        RecordingLogger<RabbitLoggerTests> logger = new RecordingLogger<RabbitLoggerTests>();

        RabbitLogger.LogCallbackException(logger, new InvalidOperationException("handler threw"));

        (LogLevel Level, string Message) entry = Assert.Single(logger.Logged);
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.Equal("ClientCallbackFailed", entry.Message);
    }

    // the category is public API in practice: it is what an operator puts in appsettings to silence the
    // client's noise without touching the bus's own logs.
    [Fact]
    public void Category_ForClientLogs_IsTheDedicatedClientCategory()
        => Assert.Equal("JorgeCostaMacia.Bus.RabbitMQ.Client", RabbitLogger.Category);
}
