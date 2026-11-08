using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// The bus's single, long-lived RabbitMQ connection, shared across the application — a connection is
/// thread-safe and meant to be reused, so it is a container-owned singleton opened lazily on first
/// use. It hands out <see cref="IChannel"/> channels on demand; channels, unlike the connection, are
/// <b>not</b> safe for concurrent use, so each caller (e.g. the scoped producer) takes its own.
/// </summary>
internal interface IConnection : IAsyncDisposable
{
    /// <summary>Opens a new channel on the shared connection.</summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Whether the wrapped connection is open — a connection never opened yet counts as open (it is
    /// opened lazily on first use), while a dropped one reports <see langword="false"/> until
    /// automatic recovery re-establishes it.
    /// </summary>
    bool IsOpen { get; }
}
