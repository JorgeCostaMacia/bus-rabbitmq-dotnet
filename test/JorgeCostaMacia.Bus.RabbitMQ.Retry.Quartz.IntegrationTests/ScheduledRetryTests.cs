using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Quartz;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.IntegrationTests;

/// <summary>
/// End-to-end proof of the Quartz-on-Postgres delayed-retry path against real ephemeral RabbitMQ and
/// Postgres containers: a command whose handler fails its first delivery is parked by the bus's error
/// handler as a durable Quartz job in the Postgres store; the job's trigger fires at the scheduled
/// time, the retry job re-produces the message to its exchange, and it redelivers to the handler,
/// which succeeds. Exercises the whole fail → schedule → persist → fire → produce-back → redeliver
/// path over the two containers.
/// </summary>
[Trait("Category", "Integration")]
public sealed class ScheduledRetryTests : IClassFixture<RetryQuartzFixture>
{
    private const string Exchange = "retry-quartz-integration-tests";
    private const string Queue = "retry-quartz-integration-tests.handler";

    // A short, positive first interval: the error handler parks the retry with scheduledAt = now + this,
    // so the trigger's first fire lands a few seconds out — the CI-feasible slice of the ladder, never
    // the five-minute repetitions that only apply once the first fire keeps failing.
    private static readonly TimeSpan _retryInterval = TimeSpan.FromSeconds(4);

    private readonly RetryQuartzFixture _fixture;

    /// <summary>Takes the shared broker + store fixture.</summary>
    /// <param name="fixture">The running RabbitMQ and Postgres containers.</param>
    public ScheduledRetryTests(RetryQuartzFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>A failed delivery is parked as a durable Quartz job in Postgres and redelivered when its trigger
    /// fires.</summary>
    [Fact]
    public async Task A_failed_delivery_is_parked_in_Postgres_and_redelivered_by_the_Quartz_scheduler()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        RetryProbe probe = new RetryProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddCommand<RetryCommand>(Exchange),
            consumer => consumer.AddCommandHandler<RetryCommand, RetryCommandHandler>(
                Queue,
                retryIntervals: ImmutableList.Create(_retryInterval)));
        builder.Services.AddQuartz(quartz => quartz.UsePersistentStore(store =>
        {
            store.UsePostgres(_fixture.PostgresConnectionString);
            store.ConfigureStore(options => options.StoreJobDataAsStrings = true);
            store.ProvisionSchema();
            store.UseSystemTextJsonSerializer();
        }));
        builder.Services.AddQuartzHostedService(options => options.WaitForJobsToComplete = true);
        builder.Services.AddRetryContext();

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            const string payload = "hello-scheduled-retry";

            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new RetryCommand(payload), cancellationToken);
            }

            bool observedParkedJob = await ObserveParkedJob(probe, cancellationToken);

            Task completed = await Task.WhenAny(
                probe.Redelivered,
                Task.Delay(TimeSpan.FromSeconds(60), cancellationToken));
            Assert.True(
                completed == probe.Redelivered,
                "The scheduled retry was not redelivered to the handler within 60 seconds.");

            // The retry came back: the durable job was persisted to the real Postgres store and fired.
            Assert.True(
                observedParkedJob,
                "The retry was never observed parked as a durable job in the Postgres store.");
            Assert.True(
                probe.Invocations >= 2,
                $"The handler ran {probe.Invocations} time(s); the original delivery plus the scheduled retry were expected.");

            // The redelivery took the scheduled delay — not an instant broker requeue — which is what
            // proves it travelled through the timed Quartz trigger and its Postgres store rather than a
            // nack-and-requeue: a failed schedule would have redelivered in milliseconds.
            Assert.True(
                probe.BetweenFirstAndSecond >= TimeSpan.FromSeconds(2.5),
                $"The retry arrived after {probe.BetweenFirstAndSecond.TotalSeconds:0.00}s; the scheduled delay was {_retryInterval.TotalSeconds:0}s.");
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Polls the Quartz store until the failed delivery shows up as a durable job — the retry parked
    /// in Postgres — or the handler has already been redelivered, or the parked window elapses.
    /// </summary>
    private async Task<bool> ObserveParkedJob(
        RetryProbe probe,
        CancellationToken cancellationToken
    )
    {
        DateTime deadline = DateTime.UtcNow.Add(TimeSpan.FromSeconds(30));

        while (DateTime.UtcNow < deadline && probe.Invocations < 2)
        {
            if (await _fixture.CountParkedJobs(cancellationToken) > 0)
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(150), cancellationToken);
        }

        return false;
    }
}
