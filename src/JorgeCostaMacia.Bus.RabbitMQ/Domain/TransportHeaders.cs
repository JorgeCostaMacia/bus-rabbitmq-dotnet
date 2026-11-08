using System.Collections.Immutable;
using System.Globalization;

namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// The header hub — the <c>jcm-</c>-prefixed header keys that carry the message envelope, which keys
/// travel as Guids/ints, how a value is rendered to its canonical header text (<see cref="ToHeader(string)"/>
/// and overloads), and how a key is re-stamped (<see cref="Restamp"/>). The RabbitMQ wire carries a
/// <c>string → string</c> table: every header travels as canonical text — Guids in their dashed "D"
/// form, ints as invariant digits, dates as ISO round-trip — so the whole envelope is human-readable in
/// the management UI, and the typed materialization happens at the reading boundary on <see cref="Transport"/>.
/// </summary>
internal static class TransportHeaders
{
    /// <summary>The common prefix shared by every envelope header key.</summary>
    public const string Prefix = "jcm-";

    public const string MessageId = Prefix + "message-id";
    public const string MessageType = Prefix + "message-type";
    public const string MessageDestinationAddress = Prefix + "message-destination-address";
    public const string MessageOriginAddress = Prefix + "message-origin-address";
    public const string MessageOccurredAt = Prefix + "message-occurred-at";
    public const string ConversationId = Prefix + "conversation-id";
    public const string ConversationAddress = Prefix + "conversation-address";
    public const string ConversationOccurredAt = Prefix + "conversation-occurred-at";
    public const string AggregateId = Prefix + "aggregate-id";
    public const string AggregateCorrelationId = Prefix + "aggregate-correlation-id";
    public const string AggregateOccurredAt = Prefix + "aggregate-occurred-at";
    public const string AggregateConsumers = Prefix + "aggregate-consumers";
    public const string RetryCount = Prefix + "retry-count";
    public const string ErrorType = Prefix + "error-type";
    public const string ErrorMessage = Prefix + "error-message";
    public const string ErrorGroupId = Prefix + "error-group-id";
    public const string ErrorOccurredAt = Prefix + "error-occurred-at";
    public const string HostMachineName = Prefix + "host-machine-name";
    public const string HostAssembly = Prefix + "host-assembly";
    public const string HostAssemblyVersion = Prefix + "host-assembly-version";
    public const string HostFrameworkVersion = Prefix + "host-framework-version";
    public const string HostBusVersion = Prefix + "host-bus-version";
    public const string HostOperatingSystemVersion = Prefix + "host-operating-system-version";

    /// <summary>The keys whose canonical text is a dashed <see cref="Guid"/> — the reader materializes them back with
    /// <c>Guid.TryParse</c>.</summary>
    public static readonly ImmutableList<string> GuidHeaders = ImmutableList.Create(
        MessageId,
        ConversationId,
        AggregateId,
        AggregateCorrelationId);

    /// <summary>The keys whose canonical text is invariant integer digits (the resilience counter).</summary>
    public static readonly ImmutableList<string> IntHeaders = ImmutableList.Create(RetryCount);

    /// <summary>Renders a text value to its header text (itself).</summary>
    public static string ToHeader(string value) => value;

    /// <summary>Renders a <see cref="Guid"/> to its header text (the canonical dashed "D" form).</summary>
    public static string ToHeader(Guid value) => value.ToString();

    /// <summary>Renders an integer to its header text (invariant decimal digits).</summary>
    public static string ToHeader(int value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>Renders a string list to its header text (comma-joined).</summary>
    public static string ToHeader(IEnumerable<string> values) => string.Join(',', values);

    /// <summary>Sets a header key to the given text on an outbound header table (replacing any existing
    /// entry).</summary>
    /// <param name="headers">The outbound header table.</param>
    /// <param name="key">The header key.</param>
    /// <param name="value">The canonical header text.</param>
    public static void Restamp(
        IDictionary<string, string> headers,
        string key,
        string value
    ) => headers[key] = value;

    /// <summary>
    /// Stamps a failure onto the (already cloned) envelope — the exception type and message, the
    /// failing queue and the UTC time — so the parked delivery is filterable and reinjectable
    /// header-side. Shared by every error and fault handler so the stamp stays identical across lanes.
    /// </summary>
    /// <param name="headers">The headers to stamp — typically a clone of the delivery's envelope.</param>
    /// <param name="error">The failure whose type and message are stamped.</param>
    /// <param name="groupId">The failing consumer queue.</param>
    public static void StampError(
        IDictionary<string, string> headers,
        Exception error,
        string groupId
    )
    {
        Type type = error.GetType();

        Restamp(headers, ErrorType, ToHeader(type.FullName ?? type.Name));
        Restamp(headers, ErrorMessage, ToHeader(error.Message));
        Restamp(headers, ErrorGroupId, ToHeader(groupId));
        Restamp(headers, ErrorOccurredAt, ToHeader(DateTime.UtcNow.ToString("O")));
    }
}
