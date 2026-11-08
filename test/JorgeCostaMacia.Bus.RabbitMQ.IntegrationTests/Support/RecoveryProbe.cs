using System.Collections.Concurrent;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// Shared, deduplicating counter for the broker-outage recovery test: the handler calls
/// <see cref="Signal"/> with each record's identity on every delivery. <see cref="UniqueHandled"/>
/// counts distinct records (so it never regresses on a redelivery) while <see cref="TotalHandled"/>
/// counts every delivery — their difference is the at-least-once redeliveries the outage produced.
/// <see cref="AllUniqueHandled"/> resolves once every expected distinct record has been seen at least
/// once (proof of no loss). Registered as a singleton so the consumer and the test share it.
/// </summary>
public sealed class RecoveryProbe
{
    private readonly ConcurrentDictionary<string, byte> _unique = new ConcurrentDictionary<string, byte>();
    private readonly TaskCompletionSource _allUniqueHandled = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private long _total;
    private long _expected = long.MaxValue;

    /// <summary>Resolves once every expected distinct record has been handled at least once.</summary>
    public Task AllUniqueHandled => _allUniqueHandled.Task;

    /// <summary>The number of distinct records handled so far — never regresses on a redelivery.</summary>
    public int UniqueHandled => _unique.Count;

    /// <summary>Every delivery counted, redeliveries included — the excess over <see cref="UniqueHandled"/> is the
    /// at-least-once redeliveries.</summary>
    public long TotalHandled => Interlocked.Read(ref _total);

    /// <summary>Arms the probe to complete once <paramref name="expected"/> distinct records have been
    /// handled.</summary>
    /// <param name="expected">The number of distinct records the batch carries.</param>
    public void Expect(long expected) => Interlocked.Exchange(ref _expected, expected);

    /// <summary>Records one delivery of <paramref name="id"/>, completing the probe when every expected distinct record
    /// has been seen.</summary>
    /// <param name="id">The delivered record's stable identity.</param>
    public void Signal(string id)
    {
        Interlocked.Increment(ref _total);
        _unique.TryAdd(id, 0);

        if (_unique.Count >= Interlocked.Read(ref _expected))
        {
            _allUniqueHandled.TrySetResult();
        }
    }
}
