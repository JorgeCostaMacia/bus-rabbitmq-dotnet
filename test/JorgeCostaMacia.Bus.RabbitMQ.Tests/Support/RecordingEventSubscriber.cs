using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>Event subscriber double — records the event it received and optionally fails, to drive the handler's
/// success and error paths.</summary>
internal sealed class RecordingEventSubscriber : EventSubscriber<TestEvent>
{
    /// <summary>The event handed to the subscriber, or <see langword="null"/> if it never ran.</summary>
    public TestEvent? Received { get; private set; }

    /// <summary>An exception to fail with, or <see langword="null"/> to succeed.</summary>
    public Exception? Failure { get; set; }

    public override Task Handle(
        EventContext<TestEvent> context,
        CancellationToken cancellationToken = default
    )
    {
        Received = context.Message;

        return Failure is not null ? Task.FromException(Failure) : Task.CompletedTask;
    }
}
