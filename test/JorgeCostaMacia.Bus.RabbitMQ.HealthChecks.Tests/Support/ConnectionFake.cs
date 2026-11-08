using RabbitMQ.Client;
using IConnection = JorgeCostaMacia.Bus.RabbitMQ.Domain.IConnection;

namespace JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Tests.Support;

/// <summary>In-memory double of the bus's shared connection — the check only reads <see cref="IsOpen"/>, so the channel
/// factory is unreachable here.</summary>
internal sealed class ConnectionFake : IConnection
{
    /// <summary>Whether the connection reports open — settable, the check's seam; defaults to open.</summary>
    public bool IsOpen { get; set; } = true;

    /// <inheritdoc />
    public Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
