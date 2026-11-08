using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>
/// Records every log written through it — the level and the rendered message — so a test can pin a
/// visibility-only behavior, where the log is the only effect there is to observe.
/// </summary>
/// <typeparam name="T">The category type the logger is created for.</typeparam>
internal sealed class RecordingLogger<T> : ILogger<T>
{
    /// <summary>The (level, message) entries logged, in order.</summary>
    public List<(LogLevel Level, string Message)> Logged { get; } = new List<(LogLevel Level, string Message)>();

    /// <inheritdoc />
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    /// <inheritdoc />
    public bool IsEnabled(LogLevel logLevel) => true;

    /// <inheritdoc />
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    ) => Logged.Add((logLevel, formatter(state, exception)));
}
