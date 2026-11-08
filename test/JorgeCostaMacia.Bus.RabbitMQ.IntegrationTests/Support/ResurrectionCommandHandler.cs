using System.Collections.Concurrent;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// Records every delivered payload into the shared queue the test inspects, so the test can assert a
/// command was handled both before the queue was deleted and after the consumer resurrected.
/// </summary>
public sealed class ResurrectionCommandHandler : CommandHandler<ResurrectionCommand>
{
    private readonly ConcurrentQueue<string> _handled;

    /// <summary>Takes the shared record of handled payloads.</summary>
    /// <param name="handled">The queue every delivered payload is appended to.</param>
    public ResurrectionCommandHandler(ConcurrentQueue<string> handled)
    {
        _handled = handled;
    }

    /// <summary>Appends the delivered payload to the shared record.</summary>
    /// <param name="context">The delivery's context.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public override Task Handle(
        CommandContext<ResurrectionCommand> context,
        CancellationToken cancellationToken = default
    )
    {
        _handled.Enqueue(context.Message.Payload);

        return Task.CompletedTask;
    }
}
