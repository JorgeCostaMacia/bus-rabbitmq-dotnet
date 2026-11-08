using System.Collections.Concurrent;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// Models the applied-state store of an idempotent consumer for the idempotency chaos test.
/// <see cref="Record"/> is called on every delivery: <see cref="Received"/> counts them all (duplicates
/// included), while the effect is applied — <see cref="Effects"/> incremented — only the first time each
/// distinct id is seen. So under the at-least-once duplicates the bus can deliver, <see cref="Effects"/>
/// stays at the distinct-record count: at-least-once + dedup = effectively-once. Registered as a
/// singleton so the consumer and the test share it.
/// </summary>
public sealed class IdempotencyProbe
{
    private readonly ConcurrentDictionary<string, byte> _applied = new ConcurrentDictionary<string, byte>();
    private long _received;
    private long _effects;

    /// <summary>Every delivery counted, duplicates included.</summary>
    public long Received => Interlocked.Read(ref _received);

    /// <summary>Distinct effects applied — one per id, no matter how many times it was delivered.</summary>
    public long Effects => Interlocked.Read(ref _effects);

    /// <summary>Records a delivery of <paramref name="id"/>, applying its effect only the first time the id is
    /// seen.</summary>
    /// <param name="id">The delivered record's stable identity.</param>
    public void Record(string id)
    {
        Interlocked.Increment(ref _received);

        if (_applied.TryAdd(id, 0))
        {
            Interlocked.Increment(ref _effects);
        }
    }
}
