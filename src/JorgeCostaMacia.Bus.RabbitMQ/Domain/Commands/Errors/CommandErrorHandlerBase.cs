using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;

/// <summary>
/// Base of the command's error handler — the bus's own error lane, invoked when the command's
/// handler fails to decide the delivery's outcome (republish to retry or park to its error queue).
/// Framework-internal: a service tunes the lane through its handler registration
/// (<c>retryIntervals</c>, <c>retryExcludeExceptionTypes</c>) and the optional retry scheduler —
/// never by replacing this handler. Closes the <see cref="IHandler{TMessage, TContext}"/> contract
/// over <see cref="CommandErrorContext{TCommand}"/>, so the error handler declares only its command
/// type and gets fully-typed access to the failed command, its envelope and the exception.
/// </summary>
/// <typeparam name="TCommand">The command type whose handling failures this handler manages.</typeparam>
/// <typeparam name="TCommandHandler">The command handler this error handler is paired with — ties the error handler to
/// its command and handler, so each pairing is a distinct type resolvable on its own.</typeparam>
internal abstract class CommandErrorHandlerBase<TCommand, TCommandHandler> :
    IHandler<TCommand, CommandErrorContext<TCommand>>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
{
    /// <summary>How the handler left the delivery — set as it runs, read by the consumer afterwards. Defaults to
    /// <see cref="ErrorResult.Unhandled"/>.</summary>
    public ErrorResult Result { get; protected set; }

    /// <summary>Handles the terminal failure of a command delivery.</summary>
    /// <param name="context">The delivery's error context — the failed command, its envelope and the exception.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(
        CommandErrorContext<TCommand> context,
        CancellationToken cancellationToken = default
    );
}
