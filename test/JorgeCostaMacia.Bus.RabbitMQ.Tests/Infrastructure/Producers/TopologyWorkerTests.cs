using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure.Producers;

public sealed class TopologyWorkerTests
{
    [Fact]
    public async Task StartAsync_MappedDirectAndFanoutExchanges_DeclaresThemWithTheConsumersOptions()
    {
        // the producer's exchanges are born at startup, idempotently, with exactly the options the
        // consumers declare with (durable, not auto-delete) — a mismatch would fail their declare.
        ChannelFake channel = new ChannelFake();
        ConnectionFake connection = new ConnectionFake(channel);
        Dictionary<string, string> exchanges = new Dictionary<string, string>()
        {
            ["orders"] = ExchangeType.Direct,
            ["orders.created"] = ExchangeType.Fanout
        };

        await new TopologyWorker(connection, exchanges).StartAsync(TestContext.Current.CancellationToken);

        Assert.Contains(("orders", ExchangeType.Direct, true, false), channel.ExchangesDeclared);
        Assert.Contains(("orders.created", ExchangeType.Fanout, true, false), channel.ExchangesDeclared);
        Assert.True(channel.Disposed);
    }

    [Fact]
    public async Task StartAsync_NoExchanges_OpensNoChannel()
    {
        ChannelFake channel = new ChannelFake();
        ConnectionFake connection = new ConnectionFake(channel);

        await new TopologyWorker(connection, new Dictionary<string, string>()).StartAsync(
            TestContext.Current.CancellationToken);

        Assert.Equal(0, connection.Created);
    }
}
