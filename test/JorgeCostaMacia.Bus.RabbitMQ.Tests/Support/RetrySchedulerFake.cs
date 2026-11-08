using JorgeCostaMacia.Bus.RabbitMQ.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>In-memory retry scheduler capturing every parked retry, with an optional failure to throw.</summary>
internal sealed class RetrySchedulerFake : IRetryScheduler
{
    public List<(
        string Exchange,
        string Queue,
        byte[] Body,
        IReadOnlyDictionary<string, string> Headers,
        DateTime ScheduledAt)> Scheduled
    { get; }
        = new List<(
            string Exchange,
            string Queue,
            byte[] Body,
            IReadOnlyDictionary<string, string> Headers,
            DateTime ScheduledAt)>();

    public Exception? Failure { get; set; }

    public Task Schedule(
        string exchange,
        string queue,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        DateTime scheduledAt,
        CancellationToken cancellationToken
    )
    {
        if (Failure is not null)
        {
            throw Failure;
        }

        Scheduled.Add((exchange, queue, body.ToArray(), headers, scheduledAt));

        return Task.CompletedTask;
    }
}
