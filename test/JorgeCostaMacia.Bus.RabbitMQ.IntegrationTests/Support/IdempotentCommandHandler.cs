using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// An idempotent handler for the idempotency chaos test: it records every delivery on the shared
/// <see cref="IdempotencyProbe"/>, which applies the record's effect only the first time its id is
/// seen. This is the consuming-side dedup the bus's at-least-once contract expects — so duplicate
/// deliveries of the same record collapse to a single effect.
/// </summary>
public sealed class IdempotentCommandHandler : CommandHandler<ChaosCommand>
{
    private readonly IdempotencyProbe _probe;

    /// <summary>Takes the shared applied-state store.</summary>
    /// <param name="probe">The idempotency probe shared with the test.</param>
    public IdempotentCommandHandler(IdempotencyProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Records the delivery on the probe, which applies the effect only once per id.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<ChaosCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        _probe.Record(context.Message.Payload);

        return Task.CompletedTask;
    }
}
