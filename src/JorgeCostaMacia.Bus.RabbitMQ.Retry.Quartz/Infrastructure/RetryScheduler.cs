using System.Globalization;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Quartz;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;

/// <summary>
/// The Quartz-backed retry scheduler: parks the delivery as a durable <see cref="RetryJob"/> with a
/// single repeating trigger — the first fire exactly at the scheduled time, then one repetition
/// every five minutes while the produce keeps failing: <see cref="Attempts"/> re-executions after
/// the first fire, the same semantics as the bus's retries.
/// </summary>
/// <remarks>
/// <para>
/// The failure path is write-free — pure Quartz lifecycle: the trigger repeats the produce on its
/// own and counts its fires, so a failed produce has nothing to schedule and nothing to persist;
/// and when the last repetition fails, Quartz completes the trigger and the job — durable — stays
/// parked in the store as the dead-letter, with no conversion write (see <see cref="RetryJob"/>).
/// </para>
/// <para>
/// The job is keyed <c>{messageId}:{retryCount}</c> in the exchange's group and parked with
/// <c>replace</c> — last write wins: an at-least-once duplicate delivery of the same failure
/// re-parks the same key, overwriting job and trigger with a fresh ladder (and reviving a
/// dead-letter parked under that key instead of silently skipping it).
/// </para>
/// </remarks>
internal sealed class RetryScheduler : IRetryScheduler
{
    private const int Attempts = 4;

    private static readonly TimeSpan _interval = TimeSpan.FromMinutes(5);

    private readonly ISchedulerFactory _schedulerFactory;

    /// <summary>Creates the scheduler over the Quartz scheduler factory.</summary>
    /// <param name="schedulerFactory">The factory resolving the configured Quartz scheduler.</param>
    public RetryScheduler(ISchedulerFactory schedulerFactory)
    {
        _schedulerFactory = schedulerFactory;
    }

    /// <inheritdoc />
    public async Task Schedule(
        string exchange,
        string queue,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        DateTime scheduledAt,
        CancellationToken cancellationToken
    )
    {
        IScheduler scheduler = await _schedulerFactory.GetScheduler(cancellationToken);

        string identity = $"{MessageId(headers)}:{RetryCount(headers)}";

        IJobDetail job = JobBuilder.Create<RetryJob>()
            .WithIdentity(identity, exchange)
            .WithDescription(queue)
            .UsingJobData(RetryJob.ExchangeKey, exchange)
            .UsingJobData(RetryJob.BodyKey, Convert.ToBase64String(body.Span))
            .UsingJobData(
                RetryJob.HeadersKey,
                JsonSerializer.Serialize(
                    headers.Select(header => new KeyValuePair<string, string>(header.Key, header.Value))))
            .StoreDurably()
            .RequestRecovery()
            .Build();

        ITrigger trigger = TriggerBuilder.Create()
            .WithIdentity(identity, exchange)
            .WithDescription(queue)
            .StartAt(new DateTimeOffset(DateTime.SpecifyKind(scheduledAt, DateTimeKind.Utc)))
            .WithSimpleSchedule(schedule => schedule.WithInterval(_interval).WithRepeatCount(Attempts))
            .Build();

        // last write wins: an at-least-once duplicate of the same failure re-parks the same key —
        // job and trigger overwritten, the ladder restarts fresh — and a dead-letter parked under
        // that key is revived by the new ladder instead of blocking the park.
        await scheduler.ScheduleJob(job, new ITrigger[] { trigger }, ScheduleJobOptions.Replacing, cancellationToken);
    }

    private static Guid MessageId(IReadOnlyDictionary<string, string> headers)
        => headers.TryGetValue(TransportHeaders.MessageId, out string? id) && Guid.TryParse(id, out Guid value)
            ? value
            : Guid.CreateVersion7();

    private static int RetryCount(IReadOnlyDictionary<string, string> headers)
        => headers.TryGetValue(TransportHeaders.RetryCount, out string? retry)
            && int.TryParse(retry, NumberStyles.Integer, CultureInfo.InvariantCulture, out int value)
            ? value
            : 0;
}
