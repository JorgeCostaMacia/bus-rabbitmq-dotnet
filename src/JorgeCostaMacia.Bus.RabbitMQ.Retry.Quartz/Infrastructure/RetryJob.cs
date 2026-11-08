using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Quartz;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;

/// <summary>
/// The parked retry's job: produces the delivery back to its exchange (with an empty routing key,
/// like the bus's own retries) each time its repeating trigger fires (see <see cref="RetryScheduler"/>).
/// </summary>
/// <remarks>
/// <para>
/// A successful produce deletes the job — pending repetitions included. A failed produce writes and
/// handles nothing: the exception just bubbles — Quartz wraps it and hands it to any job listeners —
/// and the trigger repeats the produce on its own. The dead-letter needs no code at all: with the
/// attempts exhausted Quartz completes the trigger, and the job — durable — stays parked in the
/// store, trigger-less and visible, re-firable with <c>IScheduler.TriggerJob</c> (a re-fire that
/// fails again just stays parked; one that finally succeeds deletes the job).
/// </para>
/// </remarks>
internal sealed class RetryJob : IJob
{
    public const string ExchangeKey = "exchange";
    public const string BodyKey = "body";
    public const string HeadersKey = "headers";

    private readonly IProducer _producer;

    /// <summary>Creates the job over the outbound producer.</summary>
    /// <param name="producer">The outbound gate — the retry is produced through it.</param>
    public RetryJob(IProducer producer)
    {
        _producer = producer;
    }

    /// <inheritdoc />
    public async ValueTask Execute(
        IJobExecutionContext context,
        CancellationToken cancellationToken
    )
    {
        string exchange = Exchange(context.MergedJobDataMap);
        byte[] body = Body(context.MergedJobDataMap);
        Dictionary<string, string> headers = Headers(context.MergedJobDataMap);

        // a failed produce just throws: Quartz wraps it and hands it to the job listeners, and the
        // trigger repeats the produce on its own until the attempts run out — nothing to do here.
        await _producer.Produce(exchange, string.Empty, body, headers, cancellationToken);

        // produced: the job is done for good — deleting the durable job takes any pending
        // repetition with it (and cleans up a re-fired dead-letter).
        await context.Scheduler.DeleteJob(context.JobDetail.Key, cancellationToken);
    }

    private static string Exchange(JobDataMap data)
    {
        string? value = data.GetString(ExchangeKey);

        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"Retry job data is empty '{ExchangeKey}'.");
        }

        return value;
    }

    private static byte[] Body(JobDataMap data)
    {
        string? value = data.GetString(BodyKey);

        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"Retry job data is empty '{BodyKey}'.");
        }

        return Convert.FromBase64String(value);
    }

    private static Dictionary<string, string> Headers(JobDataMap data)
    {
        string? value = data.GetString(HeadersKey);

        if (string.IsNullOrEmpty(value))
        {
            throw new InvalidOperationException($"Retry job data is empty '{HeadersKey}'.");
        }

        Dictionary<string, string> headers = new Dictionary<string, string>();

        foreach (KeyValuePair<string, string> header
            in JsonSerializer.Deserialize<List<KeyValuePair<string, string>>>(value)
            ?? new List<KeyValuePair<string, string>>())
        {
            headers[header.Key] = header.Value;
        }

        return headers;
    }
}
