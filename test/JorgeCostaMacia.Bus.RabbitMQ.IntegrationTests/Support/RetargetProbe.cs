namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The shared signal for the retry re-targeting test: it counts each subscriber's invocations separately
/// and completes a task when the failing subscriber succeeds on its immediate redelivery (its second
/// invocation). The test asserts the failing subscriber ran exactly twice (original + re-targeted retry)
/// while the other subscriber ran exactly once (the original only — the retry, targeted to the failing
/// subscriber's group via <c>AggregateConsumers</c>, is filtered out of the other subscriber).
/// </summary>
public sealed class RetargetProbe
{
    private readonly TaskCompletionSource _failingSucceeded = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _otherReceived = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);

    private int _failingInvocations;
    private int _otherInvocations;

    /// <summary>Completes when the failing subscriber succeeds on its re-targeted redelivery (its second
    /// invocation).</summary>
    public Task FailingSucceeded => _failingSucceeded.Task;

    /// <summary>Completes when the other subscriber receives its first (original) delivery.</summary>
    public Task OtherReceived => _otherReceived.Task;

    /// <summary>The number of times the failing subscriber has been invoked.</summary>
    public int FailingInvocations => Volatile.Read(ref _failingInvocations);

    /// <summary>The number of times the other subscriber has been invoked.</summary>
    public int OtherInvocations => Volatile.Read(ref _otherInvocations);

    /// <summary>Records one invocation of the failing subscriber, returning its one-based attempt number.</summary>
    /// <returns>The attempt number — <c>1</c> is the failing original, <c>2</c> the re-targeted redelivery.</returns>
    public int RecordFailing()
    {
        int attempt = Interlocked.Increment(ref _failingInvocations);

        if (attempt == 2)
        {
            _failingSucceeded.TrySetResult();
        }

        return attempt;
    }

    /// <summary>Records one invocation of the other subscriber, signalling its first (original) delivery.</summary>
    public void RecordOther()
    {
        Interlocked.Increment(ref _otherInvocations);

        _otherReceived.TrySetResult();
    }
}
