using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>Command handler double — records the command it received and optionally fails, to drive the handler's
/// success and error paths.</summary>
internal sealed class RecordingCommandHandler : CommandHandler<TestCommand>
{
    /// <summary>The command handed to the handler, or <see langword="null"/> if it never ran.</summary>
    public TestCommand? Received { get; private set; }

    /// <summary>An exception to fail with, or <see langword="null"/> to succeed.</summary>
    public Exception? Failure { get; set; }

    public override Task Handle(
        CommandContext<TestCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        Received = context.Message;

        return Failure is not null ? Task.FromException(Failure) : Task.CompletedTask;
    }
}
