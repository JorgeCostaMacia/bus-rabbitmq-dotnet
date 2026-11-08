using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;

/// <summary>
/// The container-owned outbound gate: publishes through one long-lived, confirmation-enabled
/// <see cref="IChannel"/> <b>per destination exchange</b> — opened lazily on the first publish to
/// that exchange from the shared <see cref="Domain.IConnection"/>, reused for the application's
/// lifetime, and replaced under the gate when the broker closes it. Concurrent publishes share the
/// destination's channel safely: the client pipelines them and tracks each confirmation, throttling
/// the outstanding ones on its own — so the channel count is bounded by the routing map, never by
/// the load. Being the one place every outbound byte flows through, it stamps the producing host's
/// <c>jcm-host-*</c> headers on each message (so retries and error/fault parking carry them too),
/// and mirrors the envelope's key ids onto the native AMQP <see cref="BasicProperties"/> (message
/// id, correlation, type, timestamp, app, content type) so RabbitMQ tooling and other clients see
/// them without decoding the <c>jcm-*</c> headers — which stay the source of truth the contexts read.
/// </summary>
internal sealed class Producer : Domain.IProducer, IAsyncDisposable
{
    private static readonly IReadOnlyList<KeyValuePair<string, string>> _host = Host();

    private readonly Domain.IConnection _connection;
    private readonly ILogger<Producer> _logger;
    private readonly SemaphoreSlim _gate = new SemaphoreSlim(1, 1);
    private readonly ConcurrentDictionary<string, IChannel> _channels = new ConcurrentDictionary<string, IChannel>();

    /// <summary>Creates the producer over the shared connection and the logger a failed produce is written
    /// through.</summary>
    /// <param name="connection">The shared RabbitMQ connection the destination channels are opened on.</param>
    /// <param name="logger">The logger a failed produce is written through, with the outbound delivery
    /// attached.</param>
    public Producer(
        Domain.IConnection connection,
        ILogger<Producer> logger
    )
    {
        _connection = connection;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task Produce(
        string exchange,
        string routingKey,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            IChannel channel = await ChannelAsync(exchange, cancellationToken);

            await channel.BasicPublishAsync(
                exchange,
                routingKey,
                mandatory: false,
                basicProperties: Properties(headers),
                body: body,
                cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            LogFaulted(exchange, routingKey, body, headers, exception);

            throw;
        }
    }

    /// <inheritdoc />
    public async Task Park(
        string queue,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            IChannel channel = await ChannelAsync(string.Empty, cancellationToken);

            // self-healing: the idempotent declare recreates a park queue deleted at runtime, with the
            // consumers' exact options; mandatory is the tripwire — an unroutable park throws instead of
            // being dropped (and confirmed) silently, so the failure lane can never lose a message here.
            await channel.QueueDeclareAsync(
                queue,
                durable: true,
                exclusive: false,
                autoDelete: false,
                cancellationToken: cancellationToken);
            await channel.BasicPublishAsync(
                string.Empty,
                queue,
                mandatory: true,
                basicProperties: Properties(headers),
                body: body,
                cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            LogFaulted(string.Empty, queue, body, headers, exception);

            throw;
        }
    }

    /// <summary>Logs a failed produce with the outbound delivery attached (exchange, routing key, body, envelope); the
    /// caller rethrows, so the send's task still faults.</summary>
    private void LogFaulted(
        string exchange,
        string routingKey,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        Exception exception
    )
    {
        using (BusLogger.ProducerContext(exchange, routingKey, body, headers))
        using (BusLogger.DescriptionContext(BusLoggerDescriptions.SendFaulted))
        {
            _logger.LogError(exception, "Producer failed.");
        }
    }

    /// <summary>
    /// The publish properties — persistent, JSON, the host stamped over the envelope, the key ids
    /// mirrored natively. The <c>string → string</c> envelope is copied straight into the client's
    /// <c>object?</c>-typed header table only at the <see cref="BasicProperties.Headers"/> assignment
    /// (the client encodes a string as an AMQP longstr, so it renders legibly in the management UI).
    /// </summary>
    private static BasicProperties Properties(IReadOnlyDictionary<string, string> headers)
    {
        Dictionary<string, string> stamped = new Dictionary<string, string>(headers);

        foreach (KeyValuePair<string, string> host in _host)
        {
            stamped[host.Key] = host.Value;
        }

        Dictionary<string, object?> table = new Dictionary<string, object?>(stamped.Count);

        foreach (KeyValuePair<string, string> header in stamped)
        {
            table[header.Key] = header.Value;
        }

        BasicProperties properties = new BasicProperties()
        {
            Persistent = true,
            ContentType = "application/json",
            Headers = table
        };

        Native(properties, stamped);

        return properties;
    }

    /// <summary>
    /// Mirrors the envelope's key ids from the <c>jcm-*</c> headers onto the native AMQP properties, so
    /// RabbitMQ tooling and other clients read them without decoding headers. The headers stay the
    /// source of truth; this is a write-only convenience on produce. A missing/undecodable header just
    /// leaves its property unset.
    /// </summary>
    private static void Native(
        BasicProperties properties,
        IReadOnlyDictionary<string, string> headers
    )
    {
        if (GuidText(headers, TransportHeaders.MessageId) is { } messageId)
        {
            properties.MessageId = messageId;
        }

        if (GuidText(headers, TransportHeaders.ConversationId) is { } correlationId)
        {
            properties.CorrelationId = correlationId;
        }

        if (Text(headers, TransportHeaders.MessageType) is { } type)
        {
            properties.Type = type;
        }

        if (Text(headers, TransportHeaders.HostAssembly) is { } appId)
        {
            properties.AppId = appId;
        }

        if (Text(headers, TransportHeaders.MessageOccurredAt) is { } occurredAt
            && DateTimeOffset.TryParse(
                occurredAt,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                out DateTimeOffset moment))
        {
            properties.Timestamp = new AmqpTimestamp(moment.ToUnixTimeSeconds());
        }
    }

    /// <summary>Reads a header's text, or <see langword="null"/> when absent.</summary>
    private static string? Text(
        IReadOnlyDictionary<string, string> headers,
        string key
    ) => headers.TryGetValue(key, out string? value) ? value : null;

    /// <summary>Reads a GUID header as its canonical text, or <see langword="null"/> when absent (or not valid GUID
    /// text).</summary>
    private static string? GuidText(
        IReadOnlyDictionary<string, string> headers,
        string key
    ) => headers.TryGetValue(key, out string? value) && Guid.TryParse(value, out Guid id) ? id.ToString() : null;

    /// <summary>
    /// The destination's channel — opened on the first publish to the exchange and reused for the
    /// application's lifetime; a channel the broker has closed (e.g. an async publish error) is
    /// replaced under the gate instead of handed out dead.
    /// </summary>
    private async Task<IChannel> ChannelAsync(
        string exchange,
        CancellationToken cancellationToken
    )
    {
        if (_channels.TryGetValue(exchange, out IChannel? channel) && channel.IsOpen)
        {
            return channel;
        }

        await _gate.WaitAsync(cancellationToken);

        try
        {
            if (_channels.TryGetValue(exchange, out channel) && channel.IsOpen)
            {
                return channel;
            }

            if (channel is not null)
            {
                await channel.DisposeAsync();
            }

            channel = await _connection.CreateChannelAsync(cancellationToken);

            _channels[exchange] = channel;

            return channel;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>The producing host's identity, captured once as ready-to-stamp header text.</summary>
    private static IReadOnlyList<KeyValuePair<string, string>> Host()
    {
        AssemblyName? entry = Assembly.GetEntryAssembly()?.GetName();

        return new KeyValuePair<string, string>[]
        {
            new KeyValuePair<string, string>(
                TransportHeaders.HostMachineName,
                TransportHeaders.ToHeader(Environment.MachineName)),
            new KeyValuePair<string, string>(
                TransportHeaders.HostAssembly,
                TransportHeaders.ToHeader(entry?.Name ?? "unknown")),
            new KeyValuePair<string, string>(
                TransportHeaders.HostAssemblyVersion,
                TransportHeaders.ToHeader(entry?.Version?.ToString() ?? "0.0.0")),
            new KeyValuePair<string, string>(
                TransportHeaders.HostFrameworkVersion,
                TransportHeaders.ToHeader(Environment.Version.ToString())),
            new KeyValuePair<string, string>(
                TransportHeaders.HostBusVersion,
                TransportHeaders.ToHeader(typeof(Producer).Assembly.GetName().Version?.ToString() ?? "0.0.0")),
            new KeyValuePair<string, string>(
                TransportHeaders.HostOperatingSystemVersion,
                TransportHeaders.ToHeader(Environment.OSVersion.ToString()))
        };
    }

    /// <summary>Closes every destination channel — the container disposes the singleton at shutdown.</summary>
    public async ValueTask DisposeAsync()
    {
        foreach (IChannel channel in _channels.Values)
        {
            await channel.DisposeAsync();
        }
    }
}
