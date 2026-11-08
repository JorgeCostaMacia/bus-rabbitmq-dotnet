using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The handler for <see cref="FaultCommand"/>: a no-op it never actually reaches. The fault-parking
/// test delivers a body that fails to deserialize, so the delivery breaks while building the context —
/// before any handler runs — and takes the fault lane to <c>{queue}.fault</c>.
/// </summary>
public sealed class FaultCommandHandler : CommandHandler<FaultCommand>
{
    /// <summary>Never invoked — the malformed body breaks the delivery before the handler.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<FaultCommand> context,
        CancellationToken cancellationToken = default
    ) => Task.CompletedTask;
}
