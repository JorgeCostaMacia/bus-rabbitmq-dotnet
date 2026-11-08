using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>An error handler whose <c>Handle</c> itself throws — drives the worker's error-lane endgame.</summary>
internal sealed class ThrowingCommandErrorHandler : CommandErrorHandlerBase<TestCommand, RecordingCommandHandler>
{
    public override Task Handle(
        CommandErrorContext<TestCommand> context,
        CancellationToken cancellationToken = default
    ) => throw new InvalidOperationException("error handler down");
}
