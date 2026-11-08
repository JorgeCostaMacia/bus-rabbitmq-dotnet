using System.Collections.Immutable;
using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Domain;

public sealed class TransportTests
{
    private static Transport CreateSut(params (string Key, byte[] Value)[] headers)
        => new Transport(
            headers.ToDictionary(header => header.Key, header => (object?)header.Value),
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);

    [Fact]
    public void Create_FromDeliveryArgs_MapsTheDelivery()
    {
        Transport transport = Transport.Create(
            Deliveries.Args(
                "{}"u8.ToArray(),
                new Dictionary<string, object?> { ["key"] = "value"u8.ToArray() },
                deliveryTag: 42));

        Assert.Equal(Deliveries.Exchange, transport.Exchange);
        Assert.Equal(string.Empty, transport.RoutingKey);
        Assert.Equal(42ul, transport.DeliveryTag);
        Assert.False(transport.Redelivered);
        Assert.Single(transport.Headers);
    }

    [Fact]
    public void Create_WithoutHeaders_YieldsAnEmptyTable()
    {
        Transport transport = Transport.Create(Deliveries.Args("{}"u8.ToArray(), new Dictionary<string, object?>()));

        Assert.Empty(transport.Headers);
    }

    [Fact]
    public void GetHeaderGuid_CanonicalText_RoundTrips()
    {
        Guid value = Guid.NewGuid();

        Assert.Equal(value, CreateSut(("id", Encoding.UTF8.GetBytes(value.ToString()))).GetHeaderGuid("id"));
    }

    [Fact]
    public void GetHeaderGuid_NonGuidText_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(("id", "not-a-guid"u8.ToArray())).GetHeaderGuid("id"));

    [Fact]
    public void GetHeaderString_AMissingKey_Throws()
        => Assert.Throws<KeyNotFoundException>(() => CreateSut().GetHeaderString("missing"));

    [Fact]
    public void GetHeaderString_PresentButNullValue_Throws()
    {
        // a present key whose value is null is treated as absent — the `value is null` branch throws.
        Transport transport = new Transport(
            new Dictionary<string, object?> { ["key"] = null },
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);

        Assert.Throws<KeyNotFoundException>(() => transport.GetHeaderString("key"));
    }

    [Fact]
    public void GetHeaderStringOrDefault_PresentButNullValue_ReturnsNull()
    {
        Transport transport = new Transport(
            new Dictionary<string, object?> { ["key"] = null },
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);

        Assert.Null(transport.GetHeaderStringOrDefault("key"));
    }

    [Fact]
    public void GetHeaderString_AForeignStringValue_ReadsItAsIs()
    {
        // an AMQP field table from a foreign publisher can carry a string instead of bytes.
        Transport transport = new Transport(
            new Dictionary<string, object?> { ["key"] = "value" },
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);

        Assert.Equal("value", transport.GetHeaderString("key"));
        Assert.Equal("value", transport.GetHeaderStringOrDefault("key"));
    }

    [Fact]
    public void GetHeaderString_AForeignIntValue_ReadsItsInvariantText()
    {
        // and a number: read through its invariant text, so the typed getters still work.
        Transport transport = new Transport(
            new Dictionary<string, object?> { ["key"] = 7 },
            "orders",
            string.Empty,
            deliveryTag: 10,
            redelivered: false);

        Assert.Equal("7", transport.GetHeaderString("key"));
        Assert.Equal(7, transport.GetHeaderInt("key"));
    }

    [Fact]
    public void GetHeaderStringOrDefault_AnAbsentKey_ReturnsNull()
    {
        Transport transport = CreateSut(("present", "value"u8.ToArray()));

        Assert.Equal("value", transport.GetHeaderStringOrDefault("present"));
        Assert.Null(transport.GetHeaderStringOrDefault("absent"));
    }

    [Fact]
    public void GetHeaderDateTime_RoundTripFormat_ParsesAsUtc()
    {
        DateTime value = new DateTime(2026, 7, 3, 12, 30, 45, DateTimeKind.Utc);

        DateTime parsed = CreateSut(("at", Encoding.UTF8.GetBytes(value.ToString("O")))).GetHeaderDateTime("at");

        Assert.Equal(value, parsed);
        Assert.Equal(DateTimeKind.Utc, parsed.Kind);
    }

    [Fact]
    public void GetHeaderDateTime_InvalidText_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(("at", "nope"u8.ToArray())).GetHeaderDateTime("at"));

    [Fact]
    public void GetHeaderStringList_ACommaSeparatedList_TrimsAndSkipsEmptyEntries()
        => Assert.Equal(
            new string[] { "a", "b", "c" },
            CreateSut(("list", " a, b ,,c "u8.ToArray())).GetHeaderStringList("list"));

    [Fact]
    public void GetHeaderInt_Digits_ParsesThem()
        => Assert.Equal(7, CreateSut(("count", "7"u8.ToArray())).GetHeaderInt("count"));

    [Fact]
    public void GetHeaderInt_InvalidText_Throws()
        => Assert.Throws<InvalidCastException>(() => CreateSut(("count", "nope"u8.ToArray())).GetHeaderInt("count"));

    [Fact]
    public void DecodeHeaders_AGuidAndATextHeader_RendersBothAsText()
    {
        Guid id = Guid.NewGuid();
        Transport transport = CreateSut(
            (TransportHeaders.AggregateId, Encoding.UTF8.GetBytes(id.ToString())),
            ("custom", "value"u8.ToArray()));

        ImmutableList<KeyValuePair<string, string>> decoded = transport.DecodeHeaders();

        Assert.Equal(2, decoded.Count);
        Assert.Contains(new KeyValuePair<string, string>(TransportHeaders.AggregateId, id.ToString()), decoded);
        Assert.Contains(new KeyValuePair<string, string>("custom", "value"), decoded);
    }

    [Fact]
    public void CloneHeaders_AByteHeader_CopiesItAsIndependentText()
    {
        Transport transport = CreateSut(("key", "value"u8.ToArray()));

        Dictionary<string, string> cloned = transport.CloneHeaders();

        Assert.Equal("value", cloned["key"]);

        cloned["key"] = "mutated";

        Assert.Equal("value", transport.GetHeaderString("key"));
    }
}
