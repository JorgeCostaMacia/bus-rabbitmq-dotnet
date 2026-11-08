using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The first subscriber to <see cref="FanoutEvent"/> on its own queue: signals the payload it received
/// on the shared <see cref="FanoutProbe"/>, so the test can prove its queue got a copy of the broadcast.
/// </summary>
public sealed class FirstFanoutSubscriber : EventSubscriber<FanoutEvent>
{
    private readonly FanoutProbe _probe;

    /// <summary>Takes the shared signal the test awaits.</summary>
    /// <param name="probe">The fanout signal shared with the test.</param>
    public FirstFanoutSubscriber(FanoutProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Signals the delivered event's payload to the awaiting test.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        EventContext<FanoutEvent> context,
        CancellationToken cancellationToken = default
    )
    {
        _probe.SignalFirst(context.Message.Payload);

        return Task.CompletedTask;
    }
}
