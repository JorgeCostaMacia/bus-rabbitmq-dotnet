using RabbitMQ.Client;
using IConnection = JorgeCostaMacia.Bus.RabbitMQ.Domain.IConnection;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>In-memory double of the shared connection — hands out the one <see cref="ChannelFake"/> the producer
/// publishes through.</summary>
internal sealed class ConnectionFake : IConnection
{
    private readonly ChannelFake _channel;

    /// <summary>Creates the fake over the channel it hands out.</summary>
    /// <param name="channel">The channel every <see cref="CreateChannelAsync"/> returns.</param>
    public ConnectionFake(ChannelFake channel)
    {
        _channel = channel;
    }

    /// <summary>How many channels were requested from the connection.</summary>
    public int Created { get; private set; }

    /// <summary>Whether the connection reports open — settable, the health check's seam; defaults to open.</summary>
    public bool IsOpen { get; set; } = true;

    /// <inheritdoc />
    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        Created++;

        return Task.FromResult<IChannel>(_channel);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
