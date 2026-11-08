using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Faults;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>A fault handler whose <c>Handle</c> itself throws — drives the worker's fault-lane endgame.</summary>
internal sealed class ThrowingCommandFaultHandler : CommandFaultHandlerBase<TestCommand, RecordingCommandHandler>
{
    public override Task Handle(
        CommandFaultContext context,
        CancellationToken cancellationToken = default
    ) => throw new InvalidOperationException("fault handler down");
}
