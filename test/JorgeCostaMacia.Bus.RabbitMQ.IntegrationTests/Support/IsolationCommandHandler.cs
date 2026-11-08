using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The handler for <see cref="IsolationCommand"/>: it throws for a poison record (so the bus's error
/// handler isolates it to <c>{queue}.error</c>) and counts a good record on the shared
/// <see cref="IsolationProbe"/>. A single batch mixes both, so the test can prove a failing record does
/// not stall or drop the good records around it.
/// </summary>
public sealed class IsolationCommandHandler : CommandHandler<IsolationCommand>
{
    private readonly IsolationProbe _probe;

    /// <summary>Takes the shared probe the good records are counted on.</summary>
    /// <param name="probe">The counter shared with the test.</param>
    public IsolationCommandHandler(IsolationProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Throws for a poison record; counts a good record on the probe.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<IsolationCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        if (context.Message.Payload.StartsWith("poison", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Failing a poison record on purpose so it is isolated to the error lane without stalling the batch.");
        }

        _probe.SignalGood();

        return Task.CompletedTask;
    }
}
