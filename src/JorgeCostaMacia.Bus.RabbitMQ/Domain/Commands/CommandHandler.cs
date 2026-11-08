using JorgeCostaMacia.Bus.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;

/// <summary>
/// Ergonomic base for a RabbitMQ command handler: closes the <see cref="IHandler{TMessage, TContext}"/>
/// contract over <see cref="CommandContext{TCommand}"/>, so a concrete handler declares only its
/// command type and gets fully-typed access to the message, transport and envelope.
/// <para>
/// Delivery is <b>at-least-once</b>: a handler may run more than once for the same command — an
/// un-acked delivery is redelivered, and a retry re-publishes it — and the retry path carries no
/// ordering guarantee. Handlers must therefore be <b>idempotent</b> — deduplicate by the message's id
/// or reconcile by a version (its <c>AggregateOccurredAt</c> can serve as one), never assume a once-only,
/// in-order call.
/// </para>
/// </summary>
/// <typeparam name="TCommand">The command type this handler processes.</typeparam>
public abstract class CommandHandler<TCommand> : IHandler<TCommand, CommandContext<TCommand>>
    where TCommand : Command
{
    /// <summary>Handles the delivered command.</summary>
    /// <param name="context">The delivery's context — command, transport and read-only envelope.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public abstract Task Handle(
        CommandContext<TCommand> context,
        CancellationToken cancellationToken = default
    );
}
