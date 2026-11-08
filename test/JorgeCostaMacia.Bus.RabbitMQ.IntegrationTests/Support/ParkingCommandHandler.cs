using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The handler for <see cref="ParkingCommand"/>: it always throws, so every delivery — the original
/// and each redelivered retry — fails, driving the bus's error handler down its ladder until the
/// budget is spent and the failure is parked to <c>{queue}.error</c>.
/// </summary>
public sealed class ParkingCommandHandler : CommandHandler<ParkingCommand>
{
    /// <summary>Always throws so the retry ladder exhausts to a terminal park.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<ParkingCommand> context,
        CancellationToken cancellationToken = default
    )
        => throw new InvalidOperationException(
            "Failing every delivery on purpose so the retry ladder exhausts and the failure is parked to the error queue.");
}
