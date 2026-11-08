using Quartz;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Tests.Support;

/// <summary>
/// A minimal <see cref="IJobExecutionContext"/> for firing a job in a test: it exposes the merged job
/// data map and a cancellation token (all the retry job reads). Every other member throws, so a test
/// that depends on more surface fails loudly instead of silently.
/// </summary>
internal sealed class JobExecutionContextFake : IJobExecutionContext
{
    public JobExecutionContextFake(
        JobDataMap mergedJobDataMap,
        IJobDetail? jobDetail = null,
        ITrigger? trigger = null,
        IScheduler? scheduler = null
    )
    {
        MergedJobDataMap = mergedJobDataMap;
        JobDetail = jobDetail
            ?? JobBuilder.Create<Infrastructure.RetryJob>().WithIdentity("message-1:0", "orders").Build();
        Trigger = trigger
            ?? TriggerBuilder.Create().ForJob(JobDetail).WithIdentity("message-1:0", "orders").StartNow().Build();
        _scheduler = scheduler;
    }

    private readonly IScheduler? _scheduler;

    public JobDataMap MergedJobDataMap { get; }

    public CancellationToken CancellationToken => CancellationToken.None;

    public IScheduler Scheduler => _scheduler ?? throw new NotSupportedException();
    public ITrigger Trigger { get; }
    public ICalendar? Calendar => throw new NotSupportedException();
    public bool Recovering => throw new NotSupportedException();
    public TriggerKey RecoveringTriggerKey => throw new NotSupportedException();
    public int RefireCount => throw new NotSupportedException();
    public int RetryAttempt => throw new NotSupportedException();
    public IJobDetail JobDetail { get; }
    public IJob JobInstance => throw new NotSupportedException();
    public DateTimeOffset FireTimeUtc => throw new NotSupportedException();
    public DateTimeOffset? ScheduledFireTimeUtc => throw new NotSupportedException();
    public DateTimeOffset? PreviousFireTimeUtc => throw new NotSupportedException();
    public DateTimeOffset? NextFireTimeUtc => throw new NotSupportedException();
    public string FireInstanceId => throw new NotSupportedException();
    public object? Result { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public TimeSpan JobRunTime => throw new NotSupportedException();
}
