namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The shared signal the immediate-requeue handler and the test both hold: it counts the handler's
/// invocations and completes a task on the second one — the redelivery re-published by the <c>00:00</c>
/// retry step — so the test can await the eventual success and assert the handler ran exactly twice
/// (the failing original plus the one successful immediate retry), i.e. exactly-once eventual success.
/// </summary>
public sealed class RequeueProbe
{
    private readonly TaskCompletionSource _succeeded = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);

    private int _invocations;

    /// <summary>Completes when the handler succeeds on the immediate redelivery (the second invocation).</summary>
    public Task Succeeded => _succeeded.Task;

    /// <summary>The number of times the handler has been invoked.</summary>
    public int Invocations => Volatile.Read(ref _invocations);

    /// <summary>Records one handler invocation, returning its one-based attempt number.</summary>
    /// <returns>The attempt number — <c>1</c> is the failing original, <c>2</c> the immediate redelivery.</returns>
    public int Record()
    {
        int attempt = Interlocked.Increment(ref _invocations);

        if (attempt == 2)
        {
            _succeeded.TrySetResult();
        }

        return attempt;
    }
}
