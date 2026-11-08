using JorgeCostaMacia.Bus.RabbitMQ.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;

/// <summary>
/// Opens a <see cref="ConsumerChannel"/> over the shared <see cref="IConnection"/> — the container's
/// factory each worker takes its channel from on start.
/// </summary>
internal sealed class ConsumerChannelFactory : IConsumerChannelFactory
{
    private readonly IConnection _connection;

    /// <summary>Creates the factory over the shared connection.</summary>
    /// <param name="connection">The shared RabbitMQ connection the channels are opened on.</param>
    public ConsumerChannelFactory(IConnection connection)
    {
        _connection = connection;
    }

    /// <inheritdoc />
    public async Task<IConsumerChannel> CreateAsync(CancellationToken cancellationToken = default)
        => new ConsumerChannel(await _connection.CreateChannelAsync(cancellationToken));
}
