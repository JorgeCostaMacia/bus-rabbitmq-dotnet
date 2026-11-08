using System.Collections.Immutable;
using JorgeCostaMacia.Bus.Domain.Messages;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;

/// <summary>
/// The body parked to a command's error queue when its handler fails terminally — MassTransit-style:
/// the original command embedded <b>fully typed</b> (the handler already ran, so it deserialized once —
/// the error handler reuses that instance) together with every error detail and the transport details
/// (exchange, routing key, delivery tag, redelivered, headers), so browsing the error queue shows the
/// whole failure in one place. The original envelope still travels cloned in the parked record's
/// headers (with the failure stamped on top), so reinjection tooling keeps working header-side.
/// </summary>
/// <typeparam name="TCommand">The failed command's type.</typeparam>
public sealed record CommandError<TCommand> : IErrorMessage
    where TCommand : Command
{
    /// <summary>The failure that exhausted the delivery, modeled with its whole inner-exception chain.</summary>
    public ErrorInfo Error { get; init; }

    /// <summary>UTC time the failure was parked.</summary>
    public DateTime ErrorOccurredAt { get; init; }

    /// <summary>The delivery's retry count when it exhausted.</summary>
    public int RetryCount { get; init; }

    /// <summary>The queue whose handler failed.</summary>
    public string Queue { get; init; }

    /// <summary>The host (machine) whose consumer failed — identifies the instance/replica for triage.</summary>
    public string MachineName { get; init; }

    /// <summary>The exchange the delivery failed on.</summary>
    public string Exchange { get; init; }

    /// <summary>The routing key the failed message was published with.</summary>
    public string RoutingKey { get; init; }

    /// <summary>The broker-assigned delivery tag of the failed message.</summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>Whether the failed message had been redelivered.</summary>
    public bool Redelivered { get; init; }

    /// <summary>The delivery's headers as browsable text — the envelope's binary GUID headers render as
    /// GUIDs.</summary>
    public ImmutableList<KeyValuePair<string, string>> Headers { get; init; }

    /// <summary>The original command, fully typed.</summary>
    public TCommand Message { get; init; }

    /// <summary>Creates the parked body over the failure's details and the failed command.</summary>
    public CommandError(
        ErrorInfo error,
        DateTime errorOccurredAt,
        int retryCount,
        string queue,
        string machineName,
        string exchange,
        string routingKey,
        ulong deliveryTag,
        bool redelivered,
        ImmutableList<KeyValuePair<string, string>> headers,
        TCommand message
    )
    {
        Error = error;
        ErrorOccurredAt = errorOccurredAt;
        RetryCount = retryCount;
        Queue = queue;
        MachineName = machineName;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        Headers = headers;
        Message = message;
    }

    /// <summary>Builds the parked body from the error context the handler already built — the typed command, its
    /// envelope and its failure — stamping the failing queue.</summary>
    /// <param name="context">The failed delivery's error context.</param>
    /// <param name="queue">The queue whose handler failed.</param>
    /// <returns>The body parked to the error queue.</returns>
    internal static CommandError<TCommand> Create(
        CommandErrorContext<TCommand> context,
        string queue
    )
        => new CommandError<TCommand>(
            ErrorInfo.Create(context.Error),
            DateTime.UtcNow,
            context.RetryCount,
            queue,
            Environment.MachineName,
            context.Transport.Exchange,
            context.Transport.RoutingKey,
            context.Transport.DeliveryTag,
            context.Transport.Redelivered,
            context.Transport.DecodeHeaders(),
            context.Message);
}
