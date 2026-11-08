using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using IConnection = JorgeCostaMacia.Bus.RabbitMQ.Domain.IConnection;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;

/// <summary>
/// Declares the producer's topology on startup: every exchange of the routing map is declared —
/// idempotently, with exactly the consumers' declare options — before the app starts serving, so a
/// send-only service does not depend on a consumer having been the first to create the exchange.
/// Consumers keep declaring their own topology (exchange, queue, bindings): each side
/// is self-sufficient, whoever arrives first creates it, and the other's declare is a no-op. The
/// <c>.error</c> / <c>.fault</c> park queues are no part of this — they are born lazily on the first park.
/// </summary>
internal sealed class TopologyWorker : IHostedService
{
    private readonly IConnection _connection;
    private readonly IReadOnlyDictionary<string, string> _exchanges;

    /// <summary>Creates the worker over the shared connection and the map's exchanges.</summary>
    /// <param name="connection">The shared RabbitMQ connection the declare channel is opened on.</param>
    /// <param name="exchanges">The exchange → kind (direct/fanout) map from the producer configurator.</param>
    public TopologyWorker(
        IConnection connection,
        IReadOnlyDictionary<string, string> exchanges
    )
    {
        _connection = connection;
        _exchanges = exchanges;
    }

    /// <summary>Declares every mapped exchange, idempotently, over one short-lived channel.</summary>
    /// <param name="cancellationToken">A token to cancel startup.</param>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_exchanges.Count == 0)
        {
            return;
        }

        await using IChannel channel = await _connection.CreateChannelAsync(cancellationToken);

        foreach ((string exchange, string exchangeType) in _exchanges)
        {
            await channel.ExchangeDeclareAsync(
                exchange,
                exchangeType,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);
        }
    }

    /// <summary>Nothing to stop — the topology outlives the application.</summary>
    /// <param name="cancellationToken">A token bounding the stop.</param>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
