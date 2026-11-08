using System.Collections.Specialized;
using System.Text;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;
using JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Tests.Support;
using Quartz;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Tests;

public sealed class RetryJobTests
{
    private readonly ProducerFake _producer = new ProducerFake();

    private static string EncodeHeaders(params (string Key, string Value)[] headers)
        => JsonSerializer.Serialize(
            headers.Select(header => new KeyValuePair<string, string>(header.Key, header.Value)));

    private static JobDataMap Data(
        string exchange = "orders",
        byte[]? body = null,
        string? headers = null
    )
        => new JobDataMap()
        {
            [RetryJob.ExchangeKey] = exchange,
            [RetryJob.BodyKey] = Convert.ToBase64String(body ?? "body"u8.ToArray()),
            [RetryJob.HeadersKey] = headers ?? "[]"
        };

    [Fact]
    public async Task Execute_WithAnExchangeAndABody_ProducesTheBodyWithAnEmptyRoutingKey()
    {
        await Execute(Data(exchange: "orders", body: "hello"u8.ToArray()));

        (string exchange, string routingKey, byte[] body, _) = Assert.Single(_producer.Produced);
        Assert.Equal("orders", exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal("hello", Encoding.UTF8.GetString(body));
    }

    [Fact]
    public async Task Execute_WithParkedTextHeaders_ProducesThemAsText()
    {
        await Execute(Data(headers: EncodeHeaders(("k", "v"), ("s", "text"))));

        IReadOnlyDictionary<string, string> headers = Assert.Single(_producer.Produced).Headers;
        Assert.Equal("v", headers["k"]);
        Assert.Equal("text", headers["s"]);
    }

    [Fact]
    public async Task Execute_DuplicatedHeaderKey_KeepsTheLastValue()
    {
        // the header table is a dictionary — a duplicated key in the parked list (never written
        // by the scheduler, which serializes a dictionary) collapses to its last value.
        await Execute(Data(headers: EncodeHeaders(("dup", "1"), ("dup", "2"))));

        IReadOnlyDictionary<string, string> headers = Assert.Single(_producer.Produced).Headers;
        Assert.Equal("2", headers["dup"]);
    }

    [Fact]
    public async Task Execute_NoHeaders_ProducesEmptyHeaders()
    {
        await Execute(Data(headers: "[]"));

        Assert.Empty(Assert.Single(_producer.Produced).Headers);
    }

    [Fact]
    public async Task Execute_MissingExchange_ThrowsAndProducesNothing()
    {
        JobDataMap data = Data();
        data.Remove(RetryJob.ExchangeKey);

        await Assert.ThrowsAnyAsync<Exception>(() => Execute(data));
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Execute_MissingBody_ThrowsAndProducesNothing()
    {
        JobDataMap data = Data();
        data.Remove(RetryJob.BodyKey);

        await Assert.ThrowsAnyAsync<Exception>(() => Execute(data));
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task Execute_ProduceSucceedsWithRepetitionsPending_DeletesTheJobAndItsTrigger()
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();

        await Execute(Data(), job, trigger, scheduler);

        Assert.Single(_producer.Produced);
        Assert.Null(await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken));
        Assert.Empty(await scheduler.GetTriggersOfJob(job.Key, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Execute_ProduceFails_RethrowsAndLeavesTheJobUntouched()
    {
        // the failure path is pure Quartz lifecycle: the exception bubbles (Quartz wraps it for the
        // job listeners) and the trigger repeats the produce on its own, so the job must leave the
        // store exactly as it found it — job, trigger and description intact.
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();
        _producer.Failure = new InvalidOperationException("boom");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Execute(Data(), job, trigger, scheduler));

        IJobDetail? parked = await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken);
        Assert.NotNull(parked);
        Assert.True(parked.Durable);
        Assert.Equal("orders.handler", parked.Description);
        Assert.Single(await scheduler.GetTriggersOfJob(job.Key, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Execute_RefiredDeadLetterFails_StaysParked()
    {
        // a parked dead-letter (durable, trigger-less) re-fired with IScheduler.TriggerJob that
        // fails again must let the failure bubble and stay parked, untouched.
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed(parked: true);
        _producer.Failure = new InvalidOperationException("boom");

        await Assert.ThrowsAsync<InvalidOperationException>(() => Execute(Data(), job, trigger, scheduler));

        IJobDetail? parked = await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken);
        Assert.NotNull(parked);
        Assert.True(parked.Durable);
    }

    [Fact]
    public async Task Execute_RefiredDeadLetterSucceeds_DeletesTheJob()
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed(parked: true);

        await Execute(Data(), job, trigger, scheduler);

        Assert.Single(_producer.Produced);
        Assert.Null(await scheduler.GetJobDetail(job.Key, TestContext.Current.CancellationToken));
    }

    /// <summary>Fires the job over a freshly seeded scheduler — for tests that only exercise the produce.</summary>
    private async Task Execute(JobDataMap data)
    {
        (IScheduler scheduler, IJobDetail job, ITrigger trigger) = await Seed();

        await Execute(data, job, trigger, scheduler);
    }

    private Task Execute(
        JobDataMap data,
        IJobDetail job,
        ITrigger trigger,
        IScheduler scheduler
    )
        => new RetryJob(_producer).Execute(
            new JobExecutionContextFake(data, job, trigger, scheduler),
            CancellationToken.None).AsTask();

    /// <summary>
    /// A real in-memory scheduler (never started) seeded with the durable job — with its repeating
    /// trigger, or trigger-less like a parked dead-letter.
    /// </summary>
    private static async Task<(IScheduler Scheduler, IJobDetail Job, ITrigger Trigger)> Seed(bool parked = false)
    {
        IScheduler scheduler = await QuartzSchedulerBuilder.Create().UseProperties(new NameValueCollection
        {
            ["quartz.scheduler.instanceName"] = $"test-{Guid.NewGuid():N}",
            ["quartz.jobStore.type"] = "Quartz.Simpl.RAMJobStore, Quartz",
            ["quartz.threadPool.threadCount"] = "1"
        }).Build().GetScheduler();

        IJobDetail job = JobBuilder.Create<RetryJob>()
            .WithIdentity("message-1:0", "orders")
            .WithDescription("orders.handler")
            .StoreDurably()
            .Build();

        ITrigger trigger = TriggerBuilder.Create()
            .ForJob(job)
            .WithIdentity("message-1:0", "orders")
            .StartAt(DateTimeOffset.UtcNow.AddMinutes(30))
            .WithSimpleSchedule(schedule => schedule.WithInterval(TimeSpan.FromMinutes(5)).WithRepeatCount(4))
            .Build();

        if (parked)
        {
            await scheduler.AddJob(job);
        }
        else
        {
            await scheduler.ScheduleJob(job, trigger);
        }

        return (scheduler, job, trigger);
    }
}
