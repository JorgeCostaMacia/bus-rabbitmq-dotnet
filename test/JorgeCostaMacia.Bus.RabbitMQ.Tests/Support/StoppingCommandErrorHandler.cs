using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>
/// Error handler double that triggers the worker's stop mid-delivery and then observes the stopping
/// token as a real publish would — drives the shutdown-cancellation lane (an
/// <see cref="OperationCanceledException"/> during error handling leaves the delivery unacked,
/// without nack, for the broker to requeue).
/// </summary>
internal sealed class StoppingCommandErrorHandler : CommandErrorHandlerBase<TestCommand, RecordingCommandHandler>
{
    /// <summary>Initiates the worker's stop; its task completes when the stop does.</summary>
    public Func<Task>? Stop { get; set; }

    /// <summary>The in-flight stop the handler triggered, for the test to await.</summary>
    public Task? Stopping { get; private set; }

    public override Task Handle(
        CommandErrorContext<TestCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        Stopping = Stop?.Invoke();

        cancellationToken.ThrowIfCancellationRequested();

        return Task.CompletedTask;
    }
}
