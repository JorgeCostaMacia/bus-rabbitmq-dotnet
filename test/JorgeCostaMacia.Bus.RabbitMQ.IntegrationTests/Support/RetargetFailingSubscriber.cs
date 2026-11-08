using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The subscriber that fails once: it throws on the first delivery so the bus's event error handler
/// re-publishes the event immediately, re-targeted to this subscriber's group only via the
/// <c>AggregateConsumers</c> header, then succeeds on that redelivery — recording both invocations on
/// the shared <see cref="RetargetProbe"/> the test awaits.
/// </summary>
public sealed class RetargetFailingSubscriber : EventSubscriber<RequeueEvent>
{
    private readonly RetargetProbe _probe;

    /// <summary>Takes the shared probe the subscriber records onto and the test awaits.</summary>
    /// <param name="probe">The invocation signal shared with the test.</param>
    public RetargetFailingSubscriber(RetargetProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Fails the first delivery to force an immediate, group-targeted requeue; records and succeeds on the
    /// redelivery.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        EventContext<RequeueEvent> context,
        CancellationToken cancellationToken = default
    )
    {
        int attempt = _probe.RecordFailing();

        if (attempt == 1)
        {
            throw new InvalidOperationException(
                "Failing the first delivery on purpose so the retry is re-published immediately, re-targeted to this subscriber's group only.");
        }

        return Task.CompletedTask;
    }
}
