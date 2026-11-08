using System.Collections.Specialized;
using System.Globalization;
using System.Text;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;
using Quartz;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Tests;

public sealed class RetrySchedulerTests
{
    private const string Exchange = "orders";
    private const string Queue = "orders.handler";

    // in the future on purpose: Quartz fast-forwards a repeating trigger whose start is already in
    // the past (it does not replay missed repetitions), which would break the StartAt assertions.
    private static readonly DateTime _scheduledAt = DateTime.UtcNow.AddDays(1);

    // A real in-memory Quartz scheduler, never started — ScheduleJob persists to the RAM store without firing.
    // A unique instance name keeps each test's scheduler isolated in the shared repository.
    private static ISchedulerFactory Factory()
        => QuartzSchedulerBuilder.Create().UseProperties(new NameValueCollection
        {
            ["quartz.scheduler.instanceName"] = $"test-{Guid.NewGuid():N}",
            ["quartz.jobStore.type"] = "Quartz.Simpl.RAMJobStore, Quartz",
            ["quartz.threadPool.threadCount"] = "1"
        }).Build();

    private static Dictionary<string, string> Headers(
        Guid? messageId = null,
        int? retryCount = null,
        params (string Key, string Value)[] extra
    )
    {
        Dictionary<string, string> headers = new Dictionary<string, string>();

        if (messageId is Guid id)
        {
            headers[TransportHeaders.MessageId] = id.ToString();
        }

        if (retryCount is int count)
        {
            headers[TransportHeaders.RetryCount] = count.ToString(CultureInfo.InvariantCulture);
        }

        foreach ((string key, string value) in extra)
        {
            headers[key] = value;
        }

        return headers;
    }

    [Fact]
    public async Task Schedule_AFailedDelivery_KeysTheJobByMessageIdAndRetryCountUnderTheExchange()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 2),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail? job = await scheduler.GetJobDetail(
            new JobKey($"{messageId}:2", Exchange),
            TestContext.Current.CancellationToken);

        Assert.NotNull(job);
        Assert.Equal(typeof(RetryJob), job.JobType);
        Assert.True(job.RequestsRecovery);
    }

    [Fact]
    public async Task Schedule_AFailedDelivery_ParksTheJobAsDurable()
    {
        // durable from birth: when the last repetition fails, Quartz completes the trigger — the
        // durable job must stay parked in the store as the dead-letter instead of dying with it.
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 2),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail? job = await scheduler.GetJobDetail(
            new JobKey($"{messageId}:2", Exchange),
            TestContext.Current.CancellationToken);

        Assert.NotNull(job);
        Assert.True(job.Durable);
    }

    [Fact]
    public async Task Schedule_AFailedDelivery_DescribesTheJobAndTriggerWithTheQueue()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 1),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = new JobKey($"{messageId}:1", Exchange);
        IJobDetail job = (await scheduler.GetJobDetail(key, TestContext.Current.CancellationToken))!;
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(key, TestContext.Current.CancellationToken));

        Assert.Equal(Queue, job.Description);
        Assert.Equal(Queue, trigger.Description);
    }

    [Fact]
    public async Task Schedule_AGivenUtcTime_StartsTheTriggerAtIt()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 1),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        ITrigger trigger = Assert.Single(
            await scheduler.GetTriggersOfJob(
                new JobKey($"{messageId}:1", Exchange),
                TestContext.Current.CancellationToken));

        Assert.Equal(new DateTimeOffset(_scheduledAt), trigger.StartTimeUtc);
    }

    [Fact]
    public async Task Schedule_AFailedDelivery_RepeatsTheTriggerEveryFiveMinutesUpToMaxAttempts()
    {
        // the produce re-executions ARE the trigger's own repetitions — a failed produce has
        // nothing to schedule, and Quartz itself counts the fires and completes the trigger.
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 1),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        ISimpleTrigger trigger = Assert.IsAssignableFrom<ISimpleTrigger>(
            Assert.Single(
                await scheduler.GetTriggersOfJob(
                    new JobKey($"{messageId}:1", Exchange),
                    TestContext.Current.CancellationToken)));

        Assert.Equal(TimeSpan.FromMinutes(5), trigger.RepeatInterval);
        Assert.Equal(4, trigger.RepeatCount);
    }

    [Fact]
    public async Task Schedule_AnExchangeAndABody_WritesBothIntoTheJobData()
    {
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "hello"u8.ToArray(),
            Headers(messageId, 0),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail job = (await scheduler.GetJobDetail(
            new JobKey($"{messageId}:0", Exchange),
            TestContext.Current.CancellationToken))!;

        Assert.Equal(Exchange, job.JobDataMap.GetString(RetryJob.ExchangeKey));
        Assert.Equal(
            "hello",
            Encoding.UTF8.GetString(Convert.FromBase64String(job.JobDataMap.GetString(RetryJob.BodyKey)!)));
    }

    [Fact]
    public async Task Schedule_WithTextHeaders_StoresThemAsText()
    {
        // the wire carries a string → string table, so the parked envelope round-trips as canonical
        // text — the reading boundary materializes the types back on the produce.
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 0, ("k", "v"), ("s", "text")),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        IJobDetail job = (await scheduler.GetJobDetail(
            new JobKey($"{messageId}:0", Exchange),
            TestContext.Current.CancellationToken))!;
        List<KeyValuePair<string, string>> decoded = JsonSerializer.Deserialize<List<KeyValuePair<string, string>>>(
            job.JobDataMap.GetString(RetryJob.HeadersKey)!)!;

        Assert.Contains(decoded, header => header.Key == "k" && header.Value == "v");
        Assert.Contains(decoded, header => header.Key == "s" && header.Value == "text");
    }

    [Fact]
    public async Task Schedule_TheSameDeliveryTwice_ReparksOneJobAtTheLatestTime()
    {
        // an at-least-once duplicate of the same failure must not throw nor duplicate the park: it
        // overwrites job and trigger, restarting the ladder from the newest scheduled time (which
        // also revives a dead-letter parked under the same key).
        Guid messageId = Guid.NewGuid();
        ISchedulerFactory factory = Factory();
        RetryScheduler sut = new RetryScheduler(factory);

        await sut.Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 1),
            _scheduledAt,
            TestContext.Current.CancellationToken);
        await sut.Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(messageId, 1),
            _scheduledAt.AddMinutes(10),
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = Assert.Single(
            await scheduler.GetJobKeys(
                GroupMatcher<JobKey>.GroupEquals(Exchange),
                TestContext.Current.CancellationToken));
        ITrigger trigger = Assert.Single(await scheduler.GetTriggersOfJob(key, TestContext.Current.CancellationToken));

        Assert.Equal($"{messageId}:1", key.Name);
        Assert.Equal(new DateTimeOffset(_scheduledAt.AddMinutes(10)), trigger.StartTimeUtc);
    }

    [Fact]
    public async Task Schedule_NoMessageIdHeader_FallsBackToAFreshId()
    {
        ISchedulerFactory factory = Factory();

        await new RetryScheduler(factory).Schedule(
            Exchange,
            Queue,
            "body"u8.ToArray(),
            Headers(retryCount: 0),
            _scheduledAt,
            TestContext.Current.CancellationToken);

        IScheduler scheduler = await factory.GetScheduler(TestContext.Current.CancellationToken);
        JobKey key = Assert.Single(
            await scheduler.GetJobKeys(
                GroupMatcher<JobKey>.GroupEquals(Exchange),
                TestContext.Current.CancellationToken));

        Assert.Matches("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:0$", key.Name);
    }
}
