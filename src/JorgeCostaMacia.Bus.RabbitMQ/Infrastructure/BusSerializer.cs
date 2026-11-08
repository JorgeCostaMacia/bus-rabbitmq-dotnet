using System.Text.Json;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The bus's wire format: every message body is serialized and deserialized with .NET's Web
/// defaults — camelCase property names, case-insensitive reads — shared by every produce and
/// consume site so the format stays symmetric end to end. Dictionary keys are user data and travel
/// untouched (a key policy would rewrite them on write with no reverse mapping on read).
/// </summary>
internal static class BusSerializer
{
    /// <summary>The options every body serialization uses — .NET's Web defaults (camelCase).</summary>
    public static readonly JsonSerializerOptions Options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
}
