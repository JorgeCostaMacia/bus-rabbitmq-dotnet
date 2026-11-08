using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The container-owned singleton wrapping the RabbitMQ connection: opens it lazily on first use from
/// the configured <see cref="ConnectionFactory"/> (with automatic recovery), guards the open with a
/// gate so concurrent callers share one connection, and re-opens it if it has dropped. Hands out a
/// fresh channel per request — the connection is shared and thread-safe, the channels are not. Every
/// channel is opened with <b>publisher confirmations</b> on: a publish completes only when the broker
/// has accepted the message, which is what lets the consumers ack an original only after its parked
/// copy truly exists.
/// </summary>
internal sealed class Connection : Domain.IConnection
{
    private readonly ConnectionFactory _factory;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _gate = new SemaphoreSlim(1, 1);

    private IConnection? _connection;
    private bool _disposed;

    /// <summary>Creates the connection wrapper over the configured factory.</summary>
    /// <param name="factory">The RabbitMQ connection factory (host, credentials, vhost, recovery).</param>
    /// <param name="logger">The client logger — connection shutdown/recovery/callback events log through it.</param>
    public Connection(
        ConnectionFactory factory,
        ILogger logger
    )
    {
        _factory = factory;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
    {
        IConnection connection = await OpenAsync(cancellationToken);

        // confirmations on: BasicPublishAsync completes when the broker accepts the message, not
        // when the frame hits the socket — the ack protocol of the failure lanes depends on it.
        CreateChannelOptions options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        return await connection.CreateChannelAsync(options, cancellationToken);
    }

    /// <summary>
    /// Whether the wrapped connection is open: a connection never opened yet counts as open (it is
    /// opened lazily on first use, so nothing has needed it), an open one is open, and a dropped one
    /// reports <see langword="false"/> while automatic recovery re-establishes it.
    /// </summary>
    public bool IsOpen => _connection is not { IsOpen: false };

    /// <summary>Returns the open connection, opening (or re-opening) it under the gate when needed.</summary>
    private async Task<IConnection> OpenAsync(CancellationToken cancellationToken)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _gate.WaitAsync(cancellationToken);

        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _connection = await _factory.CreateConnectionAsync(cancellationToken);

            Wire(_connection);

            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Routes the connection's client callbacks (shutdown, automatic recovery, callback exceptions) to the
    /// client logger.</summary>
    private void Wire(IConnection connection)
    {
        connection.ConnectionShutdownAsync += (_, args) =>
        {
            RabbitLogger.LogShutdown(
                _logger,
                args.ReplyCode,
                args.ReplyText,
                args.Initiator == ShutdownInitiator.Application);

            return Task.CompletedTask;
        };

        connection.RecoverySucceededAsync += (_, _) =>
        {
            RabbitLogger.LogRecovered(_logger);

            return Task.CompletedTask;
        };

        connection.ConnectionRecoveryErrorAsync += (_, args) =>
        {
            RabbitLogger.LogRecoveryError(_logger, args.Exception);

            return Task.CompletedTask;
        };

        connection.CallbackExceptionAsync += (_, args) =>
        {
            RabbitLogger.LogCallbackException(_logger, args.Exception);

            return Task.CompletedTask;
        };
    }

    /// <summary>
    /// Closes the shared connection — under the gate, so it cannot race a concurrent open, and a
    /// late caller is rejected as disposed instead of resurrecting the connection. The gate itself
    /// is not disposed: a waiter may still hold it, and an undisposed <see cref="SemaphoreSlim"/>
    /// leaks nothing.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();

        try
        {
            _disposed = true;

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
            }

            _connection = null;
        }
        finally
        {
            _gate.Release();
        }
    }
}
