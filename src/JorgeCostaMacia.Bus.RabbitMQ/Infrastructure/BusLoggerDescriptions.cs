namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The expansions stamped as the <c>BusDescription</c> scope property on every outcome log: the
/// template is the minimal, groupable fact (small cardinality), and the description expands it with
/// the explanation or consequence — what the bus did about it or what happens next.
/// </summary>
internal static class BusLoggerDescriptions
{
    public const string RepublishedToRetry = "Republished to the exchange to retry.";
    public const string ScheduledToRetry = "Scheduled to retry.";
    public const string ParkedToErrorQueue = "Parked to the error queue.";
    public const string ParkedToFaultQueue = "Parked to the fault queue.";
    public const string DeliveryNotAcked = "The delivery is not acked.";
    public const string ScheduleFailed = "The scheduling failed; the delivery is not acked.";
    public const string SendFaulted = "The send faulted.";
    public const string NackedWithRequeue = "Nacked with requeue; the broker redelivers the delivery.";
    public const string RedeliveredOnRecovery
        = "The delivery is resolved but stays unacked; the broker redelivers it on channel recovery and the idempotent handler absorbs it.";
    public const string HandedToFaultHandler = "The envelope is unreadable; handed to the fault handler.";
    public const string RetrySchedulerMissing
        = "No retry scheduler is registered; parked to the error queue as terminal.";
    public const string WorkerStopped = "The worker stopped.";
    public const string ConsumerChannelClosed
        = "The consumer channel closed; automatic recovery restores it, or the worker reopens it itself after a backoff.";
    public const string ConsumerChannelRestored
        = "The consumer channel was reopened and the topology redeclared; deliveries resume.";
}
