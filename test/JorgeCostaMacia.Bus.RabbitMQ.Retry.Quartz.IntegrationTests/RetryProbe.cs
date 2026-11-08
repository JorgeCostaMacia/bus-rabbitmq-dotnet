namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.IntegrationTests;

/// <summary>
/// The shared signal the handler and the test both hold: it counts the handler's invocations, stamps
/// the instant of the first (the failing original) and the second (the redelivery produced by the
/// Quartz job), and completes a task on that redelivery — so the test can await the whole
/// fail → schedule → fire → produce-back → redeliver path and assert both that it happened and that
/// it took the scheduled delay (never an instant broker requeue).
/// </summary>
public sealed class RetryProbe
{
    private readonly TaskCompletionSource _redelivered = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);

    private int _invocations;
    private long _firstAtTicks;
    private long _secondAtTicks;

    /// <summary>Completes when the handler is invoked the second time — the Quartz-produced redelivery.</summary>
    public Task Redelivered => _redelivered.Task;

    /// <summary>The number of times the handler has been invoked.</summary>
    public int Invocations => Volatile.Read(ref _invocations);

    /// <summary>The elapsed time between the first (failing) invocation and the second (redelivered) one.</summary>
    public TimeSpan BetweenFirstAndSecond
        => TimeSpan.FromTicks(Volatile.Read(ref _secondAtTicks) - Volatile.Read(ref _firstAtTicks));

    /// <summary>Records one handler invocation, returning its one-based attempt number.</summary>
    /// <returns>The attempt number — <c>1</c> is the original delivery, <c>2</c> the scheduled retry.</returns>
    public int Record()
    {
        int attempt = Interlocked.Increment(ref _invocations);

        if (attempt == 1)
        {
            Volatile.Write(ref _firstAtTicks, DateTime.UtcNow.Ticks);
        }
        else if (attempt == 2)
        {
            Volatile.Write(ref _secondAtTicks, DateTime.UtcNow.Ticks);
            _redelivered.TrySetResult();
        }

        return attempt;
    }
}
