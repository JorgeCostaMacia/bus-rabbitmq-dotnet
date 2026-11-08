using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The handler for <see cref="RequeueCommand"/>: it throws on the first delivery so the bus's error
/// handler re-publishes the message immediately (the <c>00:00</c> retry step), then succeeds on that
/// redelivery — recording both invocations on the shared <see cref="RequeueProbe"/> the test awaits.
/// </summary>
public sealed class RequeueCommandHandler : CommandHandler<RequeueCommand>
{
    private readonly RequeueProbe _probe;

    /// <summary>Takes the shared probe the handler records onto and the test awaits.</summary>
    /// <param name="probe">The invocation signal shared with the test.</param>
    public RequeueCommandHandler(RequeueProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Fails the first delivery to force an immediate requeue; records and succeeds on the
    /// redelivery.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<RequeueCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        int attempt = _probe.Record();

        if (attempt == 1)
        {
            throw new InvalidOperationException(
                "Failing the first delivery on purpose so the retry is re-published immediately for an instant redelivery.");
        }

        return Task.CompletedTask;
    }
}
