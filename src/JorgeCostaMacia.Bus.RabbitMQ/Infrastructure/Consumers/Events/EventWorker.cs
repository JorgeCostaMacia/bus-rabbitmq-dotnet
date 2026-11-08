using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Errors;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events.Faults;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Events;

/// <summary>
/// The consumer hosting one event subscriber: binds its queue to the event's <c>fanout</c> exchange
/// (broadcast — one queue per subscriber, each gets a copy) and, per delivery, deserializes the event
/// once and invokes the subscriber resolved from the delivery's scope. A subscriber failure goes to
/// the event's error handler, a malformed delivery (or a relayed failure) to its fault handler — both
/// resolved from the scope.
/// </summary>
/// <typeparam name="TEvent">The event type consumed.</typeparam>
/// <typeparam name="TEventSubscriber">The subscriber type resolved per delivery.</typeparam>
internal sealed class EventWorker<TEvent, TEventSubscriber> : ConsumerWorker<EventContext<TEvent>, TEventSubscriber>
    where TEvent : Event
    where TEventSubscriber : EventSubscriber<TEvent>
{
    /// <summary>Creates the event consumer over the channel factory, the scope factory, the logger and its
    /// topology.</summary>
    /// <param name="channelFactory">The factory the worker opens its channel from on start.</param>
    /// <param name="scopeFactory">The factory creating one service scope per delivered message.</param>
    /// <param name="logger">The logger for the deliveries.</param>
    /// <param name="exchange">The event's exchange.</param>
    /// <param name="queue">The queue this subscriber consumes.</param>
    /// <param name="prefetchCount">The maximum unacked messages the broker delivers before waiting for acks.</param>
    public EventWorker(
        IConsumerChannelFactory channelFactory,
        IServiceScopeFactory scopeFactory,
        ILogger<EventWorker<TEvent, TEventSubscriber>> logger,
        string exchange,
        string queue,
        ushort prefetchCount
    ) : base(channelFactory, scopeFactory, logger, exchange, ExchangeType.Fanout, queue, prefetchCount)
    {
    }

    /// <summary>
    /// Consumer-side filtering: when the event targets specific consumers
    /// (<c>AggregateConsumers</c> header non-empty — e.g. a retry re-targeted to the failing queue) and
    /// this queue is not among them, the delivery is acked and skipped without deserializing the body.
    /// </summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>Whether the delivery is skipped.</returns>
    protected override bool Filtered(BasicDeliverEventArgs args)
    {
        string? consumers = Transport.Create(args).GetHeaderStringOrDefault(TransportHeaders.AggregateConsumers);

        if (string.IsNullOrWhiteSpace(consumers))
        {
            return false;
        }

        return !consumers
            .Split(',')
            .Select(consumer => consumer.Trim())
            .Contains(Queue);
    }

    /// <inheritdoc />
    protected override EventContext<TEvent> CreateContext(BasicDeliverEventArgs args)
        => new EventContext<TEvent>(
            JsonSerializer.Deserialize<TEvent>(args.Body.Span, BusSerializer.Options)
                ?? throw new JsonException("The event body deserialized to null."),
            Transport.Create(args));

    /// <inheritdoc />
    protected override Task Handle(
        TEventSubscriber handler,
        EventContext<TEvent> context,
        CancellationToken cancellationToken
    ) => handler.Handle(context, cancellationToken);

    /// <inheritdoc />
    protected override async Task<ErrorResult> HandleError(
        IServiceProvider services,
        EventContext<TEvent> context,
        Exception exception,
        CancellationToken cancellationToken
    )
    {
        EventErrorHandlerBase<TEvent, TEventSubscriber> handler
            = services.GetRequiredService<EventErrorHandlerBase<TEvent, TEventSubscriber>>();

        await handler.Handle(
            new EventErrorContext<TEvent>(
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
        EventFaultHandlerBase<TEvent, TEventSubscriber> handler
            = services.GetRequiredService<EventFaultHandlerBase<TEvent, TEventSubscriber>>();

        await handler.Handle(
            EventFaultContext.Create(
                body,
                transport,
                exception),
            cancellationToken);

        return handler.Result;
    }
}
