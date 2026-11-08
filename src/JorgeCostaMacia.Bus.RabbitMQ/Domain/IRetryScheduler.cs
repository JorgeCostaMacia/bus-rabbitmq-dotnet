namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// Schedules a delayed retry: the parked delivery is produced back to its original exchange at the
/// given time — the envelope already carries the incremented retry count and the consumer targeting,
/// so the normal consumers receive and route it with no extra machinery. Implementations own the
/// parking mechanism and its produce re-execution policy (e.g. a Quartz job whose trigger repeats
/// until the produce lands or the attempts run out); registering one enables the positive intervals
/// of the retry ladder — without it a positive interval cannot be delayed, so it is parked to the
/// queue's <c>.error</c> as terminal.
/// </summary>
public interface IRetryScheduler
{
    /// <summary>Parks a delivery to be produced back to its exchange at the given time.</summary>
    /// <param name="exchange">The original exchange to produce back to (published with an empty routing key).</param>
    /// <param name="queue">The failing consumer queue — carried for traceability (e.g. the parked retry's
    /// description).</param>
    /// <param name="body">The raw message body.</param>
    /// <param name="headers">The envelope to travel with the retry, as canonical <c>string → string</c> text — retry
    /// count and targeting already stamped.</param>
    /// <param name="scheduledAt">The UTC time to produce the retry at.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task Schedule(
        string exchange,
        string queue,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        DateTime scheduledAt,
        CancellationToken cancellationToken
    );
}
