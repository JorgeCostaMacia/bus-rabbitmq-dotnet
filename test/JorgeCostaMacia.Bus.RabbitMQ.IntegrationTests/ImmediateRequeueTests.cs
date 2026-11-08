using System.Collections.Immutable;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// End-to-end proof of the immediate-requeue retry path (distinct from the Quartz scheduled retry): a
/// <c>00:00</c> retry step re-publishes a failed message to its exchange for an instant redelivery. The
/// handler throws on the first delivery and succeeds on the redelivery, so the test proves the handler
/// ran exactly twice and the message was eventually acked — exactly-once eventual success, no hot spin.
/// <para>
/// The event variant additionally proves the retry is re-targeted to the failing subscriber's group via
/// the <c>AggregateConsumers</c> header: the other subscriber, bound to the same fanout exchange, handled
/// the original but filters the re-targeted retry out.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class ImmediateRequeueTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "immediate-requeue-integration-tests";
    private const string Queue = "immediate-requeue-integration-tests.handler";

    private const string EventExchange = "immediate-requeue-event-integration-tests";
    private const string FailingQueue = "immediate-requeue-event-integration-tests.failing";
    private const string OtherQueue = "immediate-requeue-event-integration-tests.other";

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public ImmediateRequeueTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>A handler that fails once, with a <c>00:00</c> retry step, is redelivered immediately and succeeds —
    /// invoked exactly twice, then acked.</summary>
    [Fact]
    public async Task Send_ToAHandlerThatFailsOnce_IsRedeliveredAndSucceeds()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        RequeueProbe probe = new RequeueProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddCommand<RequeueCommand>(Exchange),
            // A single 00:00 rung: the first failure re-publishes to the exchange immediately for an
            // instant redelivery — the immediate-requeue path, no scheduler involved.
            consumer => consumer.AddCommandHandler<RequeueCommand, RequeueCommandHandler>(
                Queue,
                retryIntervals: ImmutableList.Create(TimeSpan.Zero)));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Send(new RequeueCommand("hello-immediate-requeue"), cancellationToken);
            }

            Task completed = await Task.WhenAny(
                probe.Succeeded,
                Task.Delay(TimeSpan.FromSeconds(30), cancellationToken));
            Assert.True(completed == probe.Succeeded, "The immediate retry did not succeed within 30 seconds.");

            // Settle: give any spurious third delivery a window to show up, then assert exactly-once
            // eventual success — the failing original plus the one successful redelivery, then acked.
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.Equal(2, probe.Invocations);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>An event subscriber that fails once has its immediate retry re-targeted to its own group only — the
    /// failing subscriber runs twice, the other subscriber (which handled the original) runs exactly once.</summary>
    [Fact]
    public async Task Publish_ToTwoSubscribersWhereOneFailsOnce_ReTargetsTheImmediateRetryToTheFailingSubscriberOnly()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        RetargetProbe probe = new RetargetProbe();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(probe);
        builder.Services.AddBusContext(
            _fixture.BuildConfiguration(),
            producer => producer.AddEvent<RequeueEvent>(EventExchange),
            consumer => consumer
                // The failing subscriber carries a 00:00 rung, so its first failure re-publishes the event
                // immediately, re-targeted (via AggregateConsumers) to this subscriber's group only.
                .AddEventSubscriber<RequeueEvent, RetargetFailingSubscriber>(
                    FailingQueue,
                    retryIntervals: ImmutableList.Create(TimeSpan.Zero))
                .AddEventSubscriber<RequeueEvent, RetargetOtherSubscriber>(OtherQueue));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            using (IServiceScope scope = host.Services.CreateScope())
            {
                IBus bus = scope.ServiceProvider.GetRequiredService<IBus>();
                await bus.Publish(new RequeueEvent("hello-retarget"), cancellationToken);
            }

            // Both signals must land: the failing subscriber's re-targeted retry succeeded, and the other
            // subscriber received the original — so a still-zero "other" count would be a real miss, not a race.
            Task both = Task.WhenAll(probe.FailingSucceeded, probe.OtherReceived);
            Task completed = await Task.WhenAny(both, Task.Delay(TimeSpan.FromSeconds(30), cancellationToken));
            Assert.True(
                completed == both,
                "The failing subscriber's retry and the other subscriber's original did not both arrive within 30 seconds.");

            // Settle: give the re-targeted retry a window to (wrongly) reach the other subscriber, proving
            // the AggregateConsumers filter kept it out.
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.Equal(2, probe.FailingInvocations);
            Assert.Equal(1, probe.OtherInvocations);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }
}
