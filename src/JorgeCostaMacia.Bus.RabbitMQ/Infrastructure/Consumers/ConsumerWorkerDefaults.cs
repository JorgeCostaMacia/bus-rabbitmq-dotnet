using System.Collections.Immutable;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;

/// <summary>
/// Default custom-policy settings (the retry ladder and its exclusions) a consumer falls back to
/// when the configurator is not given a value.
/// </summary>
public static class ConsumerWorkerDefaults
{
    /// <summary>Delays before each retry — <c>00:00</c> re-publishes to the exchange immediately. Default: empty (no
    /// retries).</summary>
    public static ImmutableList<TimeSpan> RetryIntervals => ImmutableList<TimeSpan>.Empty;

    /// <summary>Exception types excluded from retry. Default: empty.</summary>
    public static ImmutableList<Type> RetryExcludeExceptionTypes => ImmutableList<Type>.Empty;
}
