using Serilog.Core;
using Serilog.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>
/// Captures the events written through a Serilog pipeline so a test can assert on the properties the
/// bus pushes into the log context. The bus's logging is what makes a failure queryable — a header
/// decoded as a <see cref="Guid"/> rather than a base64 blob is the difference between finding a
/// delivery and not — so those properties are behaviour, not decoration, and this is how they are read
/// back.
/// </summary>
internal sealed class CapturingSink : ILogEventSink
{
    /// <summary>The events written, in order.</summary>
    public List<LogEvent> Events { get; } = new List<LogEvent>();

    /// <inheritdoc />
    public void Emit(LogEvent logEvent) => Events.Add(logEvent);

    /// <summary>
    /// The value of a captured property, or <see langword="null"/> when the event does not carry it. The
    /// bus pushes each decoded header as its own top-level property named after the header key, and the
    /// value comes back as the CLR type Serilog boxed — so a decoded Guid header reads as a Guid.
    /// </summary>
    /// <param name="logEvent">The captured event.</param>
    /// <param name="name">The property name.</param>
    /// <returns>The property's value, or <see langword="null"/>.</returns>
    public static object? Scalar(
        LogEvent logEvent,
        string name
    )
        => logEvent.Properties.TryGetValue(name, out LogEventPropertyValue? value) && value is ScalarValue scalar
            ? scalar.Value
            : null;
}
