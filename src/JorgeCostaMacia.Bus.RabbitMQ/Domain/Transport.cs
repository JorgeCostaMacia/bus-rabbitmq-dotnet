using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using JorgeCostaMacia.Bus.Domain;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// The RabbitMQ transport for a delivered message — the concrete <see cref="ITransport"/> a context
/// carries. Wraps the low-level AMQP delivery details (headers, exchange, routing key, delivery tag,
/// redelivered flag) and exposes typed header getters, so a context implementation can read the
/// envelope it stored in headers without re-deserializing the message body. This is the reading
/// boundary: the envelope travels the wire as canonical text, but the incoming AMQP field table hands
/// its values back as <see cref="object"/> (the client decodes a longstr to <see cref="byte"/>[], and a
/// foreign primitive stays typed), so every getter decodes to text first and then materializes the type.
/// </summary>
public sealed record Transport : ITransport
{
    /// <summary>Message headers from the AMQP field table — the raw, still-typed incoming values.</summary>
    public IReadOnlyDictionary<string, object?> Headers { get; init; }

    /// <summary>The exchange the message was published to.</summary>
    public string Exchange { get; init; }

    /// <summary>The routing key the message was published with.</summary>
    public string RoutingKey { get; init; }

    /// <summary>The broker-assigned delivery tag — the channel-scoped id used to ack/nack this delivery.</summary>
    public ulong DeliveryTag { get; init; }

    /// <summary>Whether the broker redelivered this message (a prior delivery was not acked).</summary>
    public bool Redelivered { get; init; }

    /// <summary>Initializes the transport with the broker-provided delivery details.</summary>
    /// <param name="headers">The message headers.</param>
    /// <param name="exchange">The exchange the message was published to.</param>
    /// <param name="routingKey">The routing key.</param>
    /// <param name="deliveryTag">The delivery tag used to ack/nack.</param>
    /// <param name="redelivered">Whether the message was redelivered.</param>
    public Transport(
        IReadOnlyDictionary<string, object?> headers,
        string exchange,
        string routingKey,
        ulong deliveryTag,
        bool redelivered
    )
    {
        Headers = headers;
        Exchange = exchange;
        RoutingKey = routingKey;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
    }

    /// <summary>Creates the transport for a delivered message from the consumer's delivery args.</summary>
    /// <param name="args">The delivered message.</param>
    /// <returns>The delivery's transport.</returns>
    public static Transport Create(BasicDeliverEventArgs args)
        => new Transport(
            args.BasicProperties.Headers is { } headers
                ? new Dictionary<string, object?>(headers)
                : new Dictionary<string, object?>(),
            args.Exchange,
            args.RoutingKey,
            args.DeliveryTag,
            args.Redelivered);

    /// <summary>
    /// Decodes every header to browsable text — the envelope's GUID keys render as canonical GUIDs,
    /// everything else as its decoded text. The view embedded in the bodies parked to error.
    /// </summary>
    /// <returns>Every header as a key/text pair.</returns>
    internal ImmutableList<KeyValuePair<string, string>> DecodeHeaders()
        => Headers
            .Select(header => new KeyValuePair<string, string>(header.Key, DecodeHeader(header.Key, header.Value)))
            .ToImmutableList();

    /// <summary>Decodes one header's value: a canonical GUID for the envelope's GUID keys, its decoded text
    /// otherwise.</summary>
    private static string DecodeHeader(
        string key,
        object? value
    )
    {
        string text = Decode(value);

        return TransportHeaders.GuidHeaders.Contains(key) && Guid.TryParse(text, out Guid id)
            ? id.ToString()
            : text;
    }

    /// <summary>
    /// Clones this delivery's headers, decoding each incoming value to its canonical text — the bridge
    /// from the <see cref="object"/>-typed read side to the <c>string → string</c> write side, so the
    /// result can be re-stamped for an outbound message without mutating the original.
    /// </summary>
    /// <returns>A new <c>string → string</c> header table holding every header's decoded text.</returns>
    public Dictionary<string, string> CloneHeaders()
    {
        Dictionary<string, string> clonedHeaders = new Dictionary<string, string>();

        foreach ((string key, object? value) in Headers)
        {
            clonedHeaders[key] = Decode(value);
        }

        return clonedHeaders;
    }

    /// <summary>Decodes a raw incoming header value to text: <see cref="byte"/>[] as UTF-8, a string as-is, any other
    /// primitive through its invariant text, an absent value as empty.</summary>
    /// <param name="value">The header value from the field table.</param>
    /// <returns>The value's decoded text.</returns>
    private static string Decode(object? value)
        => value switch
        {
            null => string.Empty,
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string text => text,
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
        };

    /// <summary>Reads the header with the given <paramref name="key"/> decoded to text.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value decoded to text.</returns>
    /// <exception cref="KeyNotFoundException">No header with <paramref name="key"/> is present.</exception>
    public string GetHeaderString(string key)
    {
        if (!Headers.TryGetValue(key, out object? value) || value is null)
        {
            throw new KeyNotFoundException($"The key '{key}' was not present in the headers collection.");
        }

        return Decode(value);
    }

    /// <summary>Reads the header with the given <paramref name="key"/> decoded to text, or <see langword="null"/> when
    /// the header is absent — for optional envelope fields.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value decoded to text, or <see langword="null"/> when absent.</returns>
    public string? GetHeaderStringOrDefault(string key)
        => Headers.TryGetValue(key, out object? value) && value is not null ? Decode(value) : null;

    /// <summary>Reads the header with the given <paramref name="key"/> as a <see cref="Guid"/>, parsed from its
    /// canonical text.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as a <see cref="Guid"/>.</returns>
    /// <exception cref="InvalidCastException">The header text is not a valid GUID.</exception>
    public Guid GetHeaderGuid(string key)
    {
        if (Guid.TryParse(GetHeaderString(key), out Guid value))
        {
            return value;
        }

        throw new InvalidCastException($"The key '{key}' was not a valid Guid.");
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a UTC <see cref="DateTime"/>, parsed from its
    /// ISO text.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as a UTC <see cref="DateTime"/>.</returns>
    /// <exception cref="InvalidCastException">The header text is not a valid date/time.</exception>
    public DateTime GetHeaderDateTime(string key)
    {
        if (DateTime.TryParse(
            GetHeaderString(key),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
            out DateTime value))
        {
            return value;
        }

        throw new InvalidCastException($"The key '{key}' was not a valid DateTime.");
    }

    /// <summary>Reads the header with the given <paramref name="key"/> as a comma-separated string list.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The trimmed, non-empty comma-separated values.</returns>
    public ImmutableList<string> GetHeaderStringList(string key)
        => GetHeaderString(key)
            .Split(',')
            .Select(value => value.Trim())
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .ToImmutableList();

    /// <summary>Reads the header with the given <paramref name="key"/> as an <see cref="int"/>, parsed from its
    /// invariant text.</summary>
    /// <param name="key">The header key.</param>
    /// <returns>The header value parsed as an <see cref="int"/>.</returns>
    /// <exception cref="InvalidCastException">The header text is not a valid integer.</exception>
    public int GetHeaderInt(string key)
    {
        if (int.TryParse(GetHeaderString(key), NumberStyles.Integer, CultureInfo.InvariantCulture, out int value))
        {
            return value;
        }

        throw new InvalidCastException($"The key '{key}' was not a valid int.");
    }
}
