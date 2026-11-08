using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// End-to-end proof of the error lane: a command whose handler always throws, with a retry ladder that
/// exhausts, is parked to <c>{queue}.error</c> — the terminal park. The assertion reads the message
/// back straight from the broker (over a bare connection, bypassing the bus), so it observes the parked
/// error truly sitting in the lazily-born <c>.error</c> queue, stamped with the failing queue as its
/// error group. Exercises <c>Producer.Park</c> end to end.
/// </summary>
[Trait("Category", "Integration")]
public sealed class ErrorParkingTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "error-parking-integration-tests";
    private const string Queue = "error-parking-integration-tests.handler";
    private const string ErrorQueue = Queue + ".error";

    // The wire contract of the parked error's headers (the bus's internal TransportHeaders, read here as literals).
    private const string ErrorGroupIdHeader = "jcm-error-group-id";
    private const string ErrorTypeHeader = "jcm-error-type";

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public ErrorParkingTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>A handler that always throws exhausts its retry ladder and the failure is parked to the queue's
    /// <c>.error</c>.</summary>
    [Fact]
    public async Task Send_ToAnAlwaysThrowingHandler_ParksToTheErrorQueue()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<ParkingCommand>(Exchange),
            // A single 00:00 rung: the original fails and is re-published immediately, the redelivery
            // fails again, the budget is spent — so the failure parks to .error as terminal.
            consumer => consumer.AddCommandHandler<ParkingCommand, ParkingCommandHandler>(
                Queue,
                retryIntervals: ImmutableList.Create(TimeSpan.Zero)));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        await using IConnection connection = await Broker.ConnectAsync(configuration, cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new ParkingCommand("hello-error-park"), cancellationToken);
            }

            BasicGetResult? parked = await Broker.WaitForParkedAsync(
                connection,
                ErrorQueue,
                TimeSpan.FromSeconds(45),
                cancellationToken);

            Assert.True(parked is not null, $"Nothing was parked to '{ErrorQueue}' within 45 seconds.");
            Assert.Equal(Queue, Broker.Header(parked, ErrorGroupIdHeader));
            Assert.Equal(typeof(InvalidOperationException).FullName, Broker.Header(parked, ErrorTypeHeader));
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
