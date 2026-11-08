using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// End-to-end roundtrip against a real RabbitMQ broker: opens the connection, declares the topology,
/// publishes a command with confirms, and asserts the hosted consumer deserializes it and invokes the
/// handler with the original payload. Exercises the whole produce → consume path over the container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RoundtripTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "integration-tests";
    private const string Queue = "integration-tests.handler";

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public RoundtripTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>A sent command is delivered to its handler carrying the original payload.</summary>
    [Fact]
    public async Task Send_ACommand_IsDeliveredToItsHandler()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        TaskCompletionSource<IntegrationCommand> received = new TaskCompletionSource<IntegrationCommand>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddCommand<IntegrationCommand>(Exchange),
            consumer => consumer.AddCommandHandler<IntegrationCommand, IntegrationCommandHandler>(Queue));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            const string payload = "hello-roundtrip";

            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new IntegrationCommand(payload), cancellationToken);
            }

            Task completed = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromSeconds(30), cancellationToken));
            Assert.True(completed == received.Task, "The command was not delivered to its handler within 30 seconds.");

            IntegrationCommand delivered = await received.Task;
            Assert.Equal(payload, delivered.Payload);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
