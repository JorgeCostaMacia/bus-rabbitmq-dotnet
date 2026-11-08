using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The handler for the broker-outage recovery test: records each delivery on the shared
/// <see cref="RecoveryProbe"/> (deduplicated by payload) after a small per-record delay, so the drain
/// lasts long enough for the outage to fall mid-load. It never throws — the record survives the outage,
/// not a handler failure — so the test observes pure at-least-once recovery.
/// </summary>
public sealed class RecoveryCommandHandler : CommandHandler<ChaosCommand>
{
    private static readonly TimeSpan _perRecordDelay = TimeSpan.FromMilliseconds(20);

    private readonly RecoveryProbe _probe;

    /// <summary>Takes the shared deduplicating probe.</summary>
    /// <param name="probe">The counter shared with the test.</param>
    public RecoveryCommandHandler(RecoveryProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Delays briefly, then records the delivery on the probe by its payload identity.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override async Task Handle(
        CommandContext<ChaosCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        await Task.Delay(_perRecordDelay, cancellationToken);

        _probe.Signal(context.Message.Payload);
    }
}
