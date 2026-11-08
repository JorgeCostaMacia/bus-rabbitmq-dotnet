using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// End-to-end proof of the fault lane: a malformed body — raw bytes that never deserialize to the
/// command — is delivered straight to the consumer's queue, so the delivery breaks while building the
/// context (before any handler runs) and is parked to <c>{queue}.fault</c>. The assertion reads the
/// message back straight from the broker (over a bare connection, bypassing the bus), so it observes
/// the parked fault truly sitting in the lazily-born <c>.fault</c> queue, stamped with the failing queue.
/// </summary>
[Trait("Category", "Integration")]
public sealed class FaultParkingTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "fault-parking-integration-tests";
    private const string Queue = "fault-parking-integration-tests.handler";
    private const string FaultQueue = Queue + ".fault";

    // The wire contract of the parked fault's headers (the bus's internal TransportHeaders, read here as a literal).
    private const string ErrorGroupIdHeader = "jcm-error-group-id";

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public FaultParkingTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>A malformed body that cannot be deserialized breaks the delivery and is parked to the queue's
    /// <c>.fault</c>.</summary>
    [Fact]
    public async Task Deliver_AMalformedBody_ParksToTheFaultQueue()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<FaultCommand>(Exchange),
            consumer => consumer.AddCommandHandler<FaultCommand, FaultCommandHandler>(Queue));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        await using IConnection connection = await Broker.ConnectAsync(configuration, cancellationToken);

        try
        {
            // Publish a body that will never deserialize to the command, straight to the consumer's
            // exchange (declared at worker startup) with the empty routing key its queue is bound with,
            // so the worker receives it and breaks before the handler.
            await using (IChannel channel = await Broker.ConfirmChannelAsync(connection, cancellationToken))
            {
                byte[] malformed = Encoding.UTF8.GetBytes("this-is-not-the-json-of-a-command");

                await channel.BasicPublishAsync(
                    Exchange,
                    routingKey: string.Empty,
                    mandatory: false,
                    body: malformed,
                    cancellationToken: cancellationToken);
            }

            BasicGetResult? parked = await Broker.WaitForParkedAsync(
                connection,
                FaultQueue,
                TimeSpan.FromSeconds(45),
                cancellationToken);

            Assert.True(parked is not null, $"Nothing was parked to '{FaultQueue}' within 45 seconds.");
            Assert.Equal(Queue, Broker.Header(parked, ErrorGroupIdHeader));
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
