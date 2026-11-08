using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// End-to-end proof of the event worker over a real fanout topology: one published event reaches two
/// subscribers on two separate queues, both bound to the event's fanout exchange. Exercises the whole
/// publish → fanout → two consumers path over the container — the first real coverage of the event
/// worker's fanout topology (the roundtrip suite covers only a command's direct exchange).
/// </summary>
[Trait("Category", "Integration")]
public sealed class FanoutTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "fanout-integration-tests";
    private const string FirstQueue = "fanout-integration-tests.first";
    private const string SecondQueue = "fanout-integration-tests.second";

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public FanoutTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>One published event is delivered to two subscribers on two queues bound to its fanout
    /// exchange.</summary>
    [Fact]
    public async Task Publish_AnEvent_FansOutToEverySubscriber()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        FanoutProbe probe = new FanoutProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddEvent<FanoutEvent>(Exchange),
            consumer => consumer
                .AddEventSubscriber<FanoutEvent, FirstFanoutSubscriber>(FirstQueue)
                .AddEventSubscriber<FanoutEvent, SecondFanoutSubscriber>(SecondQueue));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            const string payload = "hello-fanout";

            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Publish(new FanoutEvent(payload), cancellationToken);
            }

            Task both = Task.WhenAll(probe.First, probe.Second);
            Task completed = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(30), cancellationToken));
            Assert.True(completed == both, "The event did not reach both subscriber queues within 30 seconds.");

            Assert.Equal(payload, await probe.First);
            Assert.Equal(payload, await probe.Second);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
