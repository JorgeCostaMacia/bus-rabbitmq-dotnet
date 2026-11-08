using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Faults;

/// <summary>
/// A command's fault handler — manages a broken command delivery (undeserializable body, unreadable
/// envelope, or a failure the command error handler could not cope with): parks a
/// <see cref="CommandFault"/> to the fault queue over the raw body, transport and failure. Closes the
/// <see cref="IHandler{TMessage, TContext}"/> contract over <see cref="CommandFaultContext"/> on its
/// own. The broken delivery carries no typed message, so the parked body is raw; the type parameters
/// only tie it to its pairing — they do not shape the parked message.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
/// <typeparam name="TCommandHandler">The command handler this fault handler is paired with.</typeparam>
internal abstract class CommandFaultHandlerBase<TCommand, TCommandHandler> : IHandler<IMessage, CommandFaultContext>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
{
    /// <summary>How the handler left the delivery — set as it runs, read by the consumer afterwards. Defaults to
    /// <see cref="FaultResult.Unhandled"/>.</summary>
    public FaultResult Result { get; protected set; }

    /// <summary>Handles a broken command delivery.</summary>
    /// <param name="context">The fault context — the raw body as text, the transport and the failure.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(
        CommandFaultContext context,
        CancellationToken cancellationToken = default
    );
}
