using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// Chaos test for the at-least-once contract's consuming-side answer — idempotency. Every record is
/// sent twice, modelling the duplicate delivery the bus can produce (a redelivery after a lost ack, as
/// the broker-outage test shows happening for real). An idempotent handler dedups by the record's id,
/// so although <see cref="Records"/> × 2 deliveries arrive, exactly <see cref="Records"/> effects are
/// applied: at-least-once + dedup = effectively-once. Guards the pattern the ETL depends on to absorb
/// the duplicates the bus is allowed to deliver.
/// </summary>
[Trait("Category", "Integration")]
public sealed class IdempotencyTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "idempotency-integration-tests";
    private const string Queue = "idempotency-integration-tests.handler";
    private const int Records = 50;
    private const int Deliveries = Records * 2;

    private readonly RabbitMqFixture _fixture;
    private readonly ITestOutputHelper _output;

    /// <summary>Takes the shared broker fixture and the test output sink.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    /// <param name="output">The sink the dedup evidence is written to.</param>
    public IdempotencyTests(
        RabbitMqFixture fixture,
        ITestOutputHelper output
    )
    {
        _fixture = fixture;
        _output = output;
    }

    /// <summary>Duplicate deliveries of every record collapse to one effect each under an idempotent handler.</summary>
    [Fact]
    public async Task Send_everyRecordTwice_AppliesEachEffectExactlyOnceUnderAnIdempotentHandler()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();
        IdempotencyProbe probe = new IdempotencyProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<ChaosCommand>(Exchange),
            consumer => consumer.AddCommandHandler<ChaosCommand, IdempotentCommandHandler>(Queue));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();

                // Two rounds of the same records: the second round is the duplicate delivery the
                // at-least-once contract permits (the bus re-handing a record whose ack was lost).
                for (int round = 0; round < 2; round++)
                {
                    for (int index = 0; index < Records; index++)
                    {
                        await bus.Send(new ChaosCommand($"record-{index}"), cancellationToken);
                    }
                }
            }

            // Wait until every duplicate delivery has been received, then assert dedup held.
            await WaitUntilAsync(() => probe.Received >= Deliveries, TimeSpan.FromSeconds(180), cancellationToken);

            // Dedup held: exactly one effect per distinct record, despite every record arriving twice.
            Assert.Equal(Records, probe.Effects);
            Assert.True(
                probe.Received >= Deliveries,
                $"Expected at least {Deliveries} deliveries but saw {probe.Received}.");

            _output.WriteLine(
                $"RabbitMQ idempotency: {probe.Received} deliveries collapsed to {probe.Effects} effects ({Records} distinct records sent twice).");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>Polls <paramref name="condition"/> until it holds or the timeout elapses.</summary>
    /// <param name="condition">The condition awaited.</param>
    /// <param name="timeout">The longest time to wait.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    private static async Task WaitUntilAsync(
        Func<bool> condition,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        DateTime deadline = DateTime.UtcNow.Add(timeout);

        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Not every duplicate delivery was received within the timeout.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        }
    }
}
