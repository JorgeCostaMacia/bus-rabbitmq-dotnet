namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// Shared counter and completion signal for the failure-isolation test: the handler calls
/// <see cref="SignalGood"/> for every good record it processes, and <see cref="AllGoodHandled"/>
/// resolves once the expected number of good records has been handled — proving the poison records
/// neither stalled nor dropped the good ones. Registered as a singleton so the hosted consumer and the
/// test share it.
/// </summary>
public sealed class IsolationProbe
{
    private readonly TaskCompletionSource _allGoodHandled = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private long _goodHandled;
    private long _goodExpected = long.MaxValue;

    /// <summary>Resolves once <see cref="SignalGood"/> has been called the expected number of times.</summary>
    public Task AllGoodHandled => _allGoodHandled.Task;

    /// <summary>The number of good records handled so far.</summary>
    public long GoodHandled => Interlocked.Read(ref _goodHandled);

    /// <summary>Arms the probe to complete once <paramref name="expected"/> good records have been handled.</summary>
    /// <param name="expected">The number of good records the batch will carry.</param>
    public void Expect(long expected) => Interlocked.Exchange(ref _goodExpected, expected);

    /// <summary>Counts one handled good record, completing the probe when the expected count is reached.</summary>
    public void SignalGood()
    {
        long count = Interlocked.Increment(ref _goodHandled);

        if (count >= Interlocked.Read(ref _goodExpected))
        {
            _allGoodHandled.TrySetResult();
        }
    }
}
