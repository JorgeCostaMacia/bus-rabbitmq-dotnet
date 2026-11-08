using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Faults;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Commands;

/// <summary>
/// The consumer hosting one command handler: binds its queue to the command's <c>direct</c> exchange
/// (point-to-point — one queue) and, per delivery, deserializes the command once and invokes the
/// handler resolved from the delivery's scope. A handler failure goes to the command's error handler,
/// a malformed delivery (or a relayed failure) to its fault handler — both resolved from the scope.
/// </summary>
/// <typeparam name="TCommand">The command type consumed.</typeparam>
/// <typeparam name="TCommandHandler">The handler type resolved per delivery.</typeparam>
internal sealed class CommandWorker<TCommand, TCommandHandler> :
    ConsumerWorker<CommandContext<TCommand>, TCommandHandler>
    where TCommand : Command
    where TCommandHandler : CommandHandler<TCommand>
{
    /// <summary>Creates the command consumer over the channel factory, the scope factory, the logger and its
    /// topology.</summary>
    /// <param name="channelFactory">The factory the worker opens its channel from on start.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="exchange">The command's exchange.</param>
    /// <param name="queue">The queue this handler consumes.</param>
    /// <param name="prefetchCount">The maximum unacked messages the broker delivers before waiting for acks.</param>
    public CommandWorker(
        IConsumerChannelFactory channelFactory,
        IServiceScopeFactory scopeFactory,
        ILogger<CommandWorker<TCommand, TCommandHandler>> logger,
        string exchange,
        string queue,
        ushort prefetchCount
    ) : base(channelFactory, scopeFactory, logger, exchange, ExchangeType.Direct, queue, prefetchCount)
    {
    }

    /// <inheritdoc />
    protected override CommandContext<TCommand> CreateContext(BasicDeliverEventArgs args)
        => new CommandContext<TCommand>(
            JsonSerializer.Deserialize<TCommand>(args.Body.Span, BusSerializer.Options)
                ?? throw new JsonException("The command body deserialized to null."),
            Transport.Create(args));

    /// <inheritdoc />
    protected override Task Handle(
        TCommandHandler handler,
        CommandContext<TCommand> context,
        CancellationToken cancellationToken
    ) => handler.Handle(context, cancellationToken);

    /// <inheritdoc />
    protected override async Task<ErrorResult> HandleError(
        IServiceProvider services,
        CommandContext<TCommand> context,
        Exception exception,
        CancellationToken cancellationToken
    )
    {
        CommandErrorHandlerBase<TCommand, TCommandHandler> handler
            = services.GetRequiredService<CommandErrorHandlerBase<TCommand, TCommandHandler>>();

        await handler.Handle(
            new CommandErrorContext<TCommand>(
                context.Message,
                context.Transport,
                exception),
            cancellationToken);

        return handler.Result;
    }

    /// <inheritdoc />
    protected override async Task<FaultResult> HandleFault(
        IServiceProvider services,
        ReadOnlyMemory<byte> body,
        Transport transport,
        Exception exception,
        CancellationToken cancellationToken
    )
    {
        CommandFaultHandlerBase<TCommand, TCommandHandler> handler
            = services.GetRequiredService<CommandFaultHandlerBase<TCommand, TCommandHandler>>();

        await handler.Handle(
            CommandFaultContext.Create(
                body,
                transport,
                exception),
            cancellationToken);

        return handler.Result;
    }
}
