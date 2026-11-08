using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// The handler for <see cref="IntegrationCommand"/>: completes the injected
/// <see cref="TaskCompletionSource{TResult}"/> with the delivered command, so the test can await the
/// real broker → consumer → handler path and assert on what arrived.
/// </summary>
public sealed class IntegrationCommandHandler : CommandHandler<IntegrationCommand>
{
    private readonly TaskCompletionSource<IntegrationCommand> _received;

    /// <summary>Takes the shared signal the test awaits.</summary>
    /// <param name="received">The completion source signalled on delivery.</param>
    public IntegrationCommandHandler(TaskCompletionSource<IntegrationCommand> received)
    {
        _received = received;
    }

    /// <summary>Signals the delivered command to the awaiting test.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<IntegrationCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        _received.TrySetResult(context.Message);

        return Task.CompletedTask;
    }
}
