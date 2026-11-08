using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;

/// <summary>
/// The send side of the bus's configuration: maps each message this service sends or publishes to its
/// exchange (<see cref="AddCommand{TCommand}"/> / <see cref="AddEvent{TEvent}"/>). It owns the
/// type → exchange routing map — the single source of truth for a message's exchange — which the bus
/// reads to publish and the consumer side reads to bind its queues. The connection is shared (bound
/// once from <c>Bus:Connection</c>), so it is not configured here.
/// </summary>
public sealed class ProducerConfigurator
{
    private readonly Dictionary<Type, string> _messages = new Dictionary<Type, string>();
    private readonly Dictionary<string, string> _exchanges = new Dictionary<string, string>();

    /// <summary>Creates an empty configurator.</summary>
    internal ProducerConfigurator() { }

    /// <summary>The type → exchange routing map the bus publishes through and the consumers bind from.</summary>
    internal IReadOnlyDictionary<Type, string> Messages => _messages;

    /// <summary>The exchange → kind (direct/fanout) map the startup topology declarer creates.</summary>
    internal IReadOnlyDictionary<string, string> Exchanges => _exchanges;

    /// <summary>Maps a command this service sends to its exchange.</summary>
    /// <typeparam name="TCommand">The command type.</typeparam>
    /// <param name="exchange">The exchange the command is sent to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ProducerConfigurator AddCommand<TCommand>(string exchange)
        where TCommand : Command
    {
        if (!_messages.TryAdd(typeof(TCommand), exchange))
        {
            throw new InvalidOperationException($"'{typeof(TCommand).FullName}' is already mapped to an exchange.");
        }

        RegisterExchange(exchange, ExchangeType.Direct);

        return this;
    }

    /// <summary>Maps an event this service publishes to its exchange.</summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="exchange">The exchange the event is published to.</param>
    /// <returns>The same configurator, to allow method chaining.</returns>
    public ProducerConfigurator AddEvent<TEvent>(string exchange)
        where TEvent : Event
    {
        if (!_messages.TryAdd(typeof(TEvent), exchange))
        {
            throw new InvalidOperationException($"'{typeof(TEvent).FullName}' is already mapped to an exchange.");
        }

        RegisterExchange(exchange, ExchangeType.Fanout);

        return this;
    }

    /// <summary>
    /// Records the exchange's kind for the startup declarer — an exchange cannot be a command's
    /// (direct) and an event's (fanout) at once: the broker would reject the second declare, so the
    /// misconfiguration surfaces here, at registration, with a readable message.
    /// </summary>
    private void RegisterExchange(
        string exchange,
        string exchangeType
    )
    {
        if (_exchanges.TryGetValue(exchange, out string? registered) && registered != exchangeType)
        {
            throw new InvalidOperationException(
                $"'{exchange}' is already mapped as a '{registered}' exchange; commands (direct) and events (fanout) cannot share one.");
        }

        _exchanges[exchange] = exchangeType;
    }
}
