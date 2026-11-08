using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Faults;
using Microsoft.Extensions.Logging;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Commands;

/// <summary>
/// The default command fault handler — parks a <see cref="CommandFault"/> to the command queue's
/// <c>.fault</c> over the raw, never-deserialized body, the envelope cloned in the headers with the
/// failure stamped on top. Runs for a malformed command delivery, and as the relay when the command
/// error handler could not cope. Reports through <c>Result</c>: <see cref="FaultResult.Parked"/>
/// acks, <see cref="FaultResult.Unhandled"/> leaves it to redeliver. Never throws for control flow.
/// </summary>
/// <typeparam name="TCommand">The command type.</typeparam>
/// <typeparam name="TCommandHandler">The command handler it is paired with.</typeparam>
internal sealed class CommandFaultHandler<TCommand, TCommandHandler> :
    CommandFaultHandlerBase<TCommand, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
{
    private const string FaultQueueSuffix = ".fault";

    private readonly IProducer _producer;
    private readonly ILogger _logger;
    private readonly string _queue;

    /// <summary>Creates the handler over the outbound producer, the logger and the consumer's queue.</summary>
    /// <param name="producer">The outbound gate — the fault parking goes through it.</param>
    /// <param name="logger">The consumer's logger.</param>
    /// <param name="queue">The consumer queue — faults park to its <c>.fault</c>, and it is stamped on the parked fault
    /// as the failing queue.</param>
    public CommandFaultHandler(
        IProducer producer,
        ILogger logger,
        string queue
    )
    {
        _producer = producer;
        _logger = logger;
        _queue = queue;
    }

    /// <inheritdoc />
    public override async Task Handle(
        CommandFaultContext context,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            CommandFault fault = CommandFault.Create(
                context,
                _queue);

            await _producer.Park(
                _queue + FaultQueueSuffix,
                JsonSerializer.SerializeToUtf8Bytes(fault, BusSerializer.Options),
                FaultHeaders(context),
                cancellationToken);

            using (BusLogger.DescriptionContext(BusLoggerDescriptions.ParkedToFaultQueue))
            {
                _logger.LogError(context.Error, "Delivery faulted.");
            }

            Result = FaultResult.Parked;
        }
        catch (OperationCanceledException)
        {
            Result = FaultResult.Unhandled;
        }
        catch (Exception park)
        {
            using (BusLogger.DescriptionContext(BusLoggerDescriptions.DeliveryNotAcked))
            {
                _logger.LogError(park, "Parking failed.");
            }

            Result = FaultResult.Unhandled;
        }
    }

    /// <summary>Clones the delivery's envelope and stamps the failure on top (exception type/message, the failing
    /// queue, the UTC time) — filterable and reinjectable header-side.</summary>
    private Dictionary<string, string> FaultHeaders(CommandFaultContext context)
    {
        Dictionary<string, string> headers = context.Transport.CloneHeaders();

        TransportHeaders.StampError(headers, context.Error, _queue);

        return headers;
    }
}
