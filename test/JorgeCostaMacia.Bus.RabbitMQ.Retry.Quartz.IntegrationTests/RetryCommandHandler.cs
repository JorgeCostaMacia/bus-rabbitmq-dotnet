using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.IntegrationTests;

/// <summary>
/// The handler for <see cref="RetryCommand"/>: it fails the first delivery on purpose so the bus's
/// error handler parks a delayed retry through the Quartz-backed <c>IRetryScheduler</c>, and succeeds
/// on the second — the redelivery the Quartz job produces back to the exchange when the trigger fires.
/// It records both invocations on the shared <see cref="RetryProbe"/> the test awaits.
/// </summary>
public sealed class RetryCommandHandler : CommandHandler<RetryCommand>
{
    private readonly RetryProbe _probe;

    /// <summary>Takes the shared probe the handler records onto and the test awaits.</summary>
    /// <param name="probe">The invocation signal shared with the test.</param>
    public RetryCommandHandler(RetryProbe probe)
    {
        _probe = probe;
    }

    /// <summary>Fails the first delivery to force a scheduled retry; records and succeeds on the redelivery.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<RetryCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        int attempt = _probe.Record();

        if (attempt == 1)
        {
            throw new InvalidOperationException(
                "Failing the first delivery on purpose so the retry is parked as a delayed Quartz job.");
        }

        return Task.CompletedTask;
    }
}
