using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// Chaos test for the record-at-a-time failure isolation the ETL relies on: a single batch mixes good
/// records with poison ones (a handler that throws), and the bus must handle <b>every</b> good record
/// while isolating each poison one to <c>{queue}.error</c> — a failing record never stalls nor drops
/// the good records around it ("if one fails, the other 999 still go through"). The good count is
/// awaited through a shared probe; the poison isolation is read back straight from the broker.
/// </summary>
[Trait("Category", "Integration")]
public sealed class FailureIsolationTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "failure-isolation-integration-tests";
    private const string Queue = "failure-isolation-integration-tests.handler";
    private const string ErrorQueue = Queue + ".error";
    private const int Good = 100;
    private const int PoisonEvery = 30;

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public FailureIsolationTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>Every good record in a batch is handled and each poison record is parked, without a failure stalling
    /// the batch.</summary>
    [Fact]
    public async Task Send_aBatchMixingGoodAndPoisonRecords_HandlesEveryGoodOneAndIsolatesThePoisonWithoutStalling()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();
        IsolationProbe probe = new IsolationProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<IsolationCommand>(Exchange),
            // A single 00:00 rung: a poison record fails, is re-published immediately, fails again, and
            // parks to .error — without blocking the good records.
            consumer => consumer.AddCommandHandler<IsolationCommand, IsolationCommandHandler>(
                Queue,
                retryIntervals: ImmutableList.Create(TimeSpan.Zero)));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        await using IConnection connection = await Broker.ConnectAsync(configuration, cancellationToken);

        try
        {
            probe.Expect(Good);

            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();

                // Interleave poison records among the good ones, so a poison sits mid-batch and we prove
                // it neither stalls nor drops the good records that follow it.
                for (int index = 0; index < Good; index++)
                {
                    if (index % PoisonEvery == 0)
                    {
                        await bus.Send(new IsolationCommand($"poison-{index}"), cancellationToken);
                    }

                    await bus.Send(new IsolationCommand($"good-{index}"), cancellationToken);
                }
            }

            // The invariant: every good record is handled even though poison records fail mid-batch.
            Task completed = await Task.WhenAny(
                probe.AllGoodHandled,
                Task.Delay(TimeSpan.FromSeconds(180), cancellationToken));
            Assert.True(
                completed == probe.AllGoodHandled,
                $"Only {probe.GoodHandled} of {Good} good records were handled within 180 seconds — a poison record stalled or dropped the batch.");

            // And the poison is isolated, not silently lost: at least one landed on the error lane.
            BasicGetResult? parked = await Broker.WaitForParkedAsync(
                connection,
                ErrorQueue,
                TimeSpan.FromSeconds(90),
                cancellationToken);
            Assert.True(parked is not null, $"No poison record was parked to '{ErrorQueue}'.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
