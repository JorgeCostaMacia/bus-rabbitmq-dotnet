using System.Reflection;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure;

/// <summary>
/// Pins the bus log descriptions to their exact text. They feed Loki/Grafana dashboards and alerts, so
/// a reworded, renamed, added or removed description must be a deliberate change — this test fails until
/// the pinned set below is updated to match — and it guards that every description stays distinct.
/// </summary>
public sealed class BusLoggerDescriptionsTests
{
    private static readonly Dictionary<string, string> _expected = new Dictionary<string, string>()
    {
        ["RepublishedToRetry"] = "Republished to the exchange to retry.",
        ["ScheduledToRetry"] = "Scheduled to retry.",
        ["ParkedToErrorQueue"] = "Parked to the error queue.",
        ["ParkedToFaultQueue"] = "Parked to the fault queue.",
        ["DeliveryNotAcked"] = "The delivery is not acked.",
        ["ScheduleFailed"] = "The scheduling failed; the delivery is not acked.",
        ["SendFaulted"] = "The send faulted.",
        ["NackedWithRequeue"] = "Nacked with requeue; the broker redelivers the delivery.",
        ["RedeliveredOnRecovery"]
            = "The delivery is resolved but stays unacked; the broker redelivers it on channel recovery and the idempotent handler absorbs it.",
        ["HandedToFaultHandler"] = "The envelope is unreadable; handed to the fault handler.",
        ["RetrySchedulerMissing"] = "No retry scheduler is registered; parked to the error queue as terminal.",
        ["WorkerStopped"] = "The worker stopped.",
        ["ConsumerChannelClosed"]
            = "The consumer channel closed; automatic recovery restores it, or the worker reopens it itself after a backoff.",
        ["ConsumerChannelRestored"]
            = "The consumer channel was reopened and the topology redeclared; deliveries resume."
    };

    [Fact]
    public void Descriptions_MatchTheirPinnedText()
    {
        Dictionary<string, string> actual = Descriptions();

        Assert.Equal(_expected.Count, actual.Count);

        foreach ((string name, string text) in actual)
        {
            Assert.True(
                _expected.TryGetValue(name, out string? expected),
                $"Unpinned description '{name}' — add it to the pinned set (and update any Loki/Grafana query that keys on it).");
            Assert.Equal(expected, text);
        }
    }

    [Fact]
    public void Descriptions_AreDistinct()
    {
        string[] values = Descriptions().Values.ToArray();

        Assert.Equal(values.Length, values.Distinct().Count());
    }

    /// <summary>Reads every public string constant off the descriptions holder by reflection.</summary>
    private static Dictionary<string, string> Descriptions()
        => typeof(BusLoggerDescriptions)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field is { IsLiteral: true, IsInitOnly: false } && field.FieldType == typeof(string))
            .ToDictionary(field => field.Name, field => (string)field.GetValue(null)!);
}
