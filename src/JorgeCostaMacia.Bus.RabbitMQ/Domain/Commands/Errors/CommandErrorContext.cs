using JorgeCostaMacia.Bus.Domain.Contexts;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;

/// <summary>
/// The command context an error handler receives when a delivery fails terminally — the delivery's
/// <see cref="CommandContext{TCommand}"/> itself (the handler already ran, so the command deserializes)
/// extended with the <see cref="IErrorContext{TError}"/> facet carrying the failure. In-process only,
/// never serialized.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
public sealed record CommandErrorContext<TCommand> :
    CommandContext<TCommand>,
    IErrorContext<Exception>
    where TCommand : Command
{
    /// <summary>The failure that exhausted the delivery.</summary>
    public Exception Error { get; init; }

    /// <summary>Builds the error context over the failed command, its transport and its failure.</summary>
    /// <param name="message">The command payload.</param>
    /// <param name="transport">The RabbitMQ transport for this delivery.</param>
    /// <param name="error">The failure that exhausted the delivery.</param>
    public CommandErrorContext(
        TCommand message,
        Transport transport,
        Exception error
    ) : base(message, transport)
    {
        Error = error;
    }
}
