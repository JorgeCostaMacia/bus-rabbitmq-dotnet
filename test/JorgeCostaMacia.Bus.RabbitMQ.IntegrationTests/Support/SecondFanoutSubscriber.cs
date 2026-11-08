using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The second subscriber to <see cref="FanoutEvent"/> on its own separate queue: signals the payload
/// it received on the shared <see cref="FanoutProbe"/>, so the test can prove that a second queue,
/// bound to the same fanout exchange, got its own copy of the one published event.
/// </summary>
public sealed class SecondFanoutSubscriber : EventSubscriber<FanoutEvent>
{
    private readonly FanoutProbe _probe;

    /// <summary>Takes the shared signal the test awaits.</summary>
    /// <param name="probe">The fanout signal shared with the test.</param>
    public SecondFanoutSubscriber(FanoutProbe probe)
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
        _probe.SignalSecond(context.Message.Payload);

        return Task.CompletedTask;
    }
}
