using System.Collections.Concurrent;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// The consumer recovers on its own after a broker interruption: with the worker running, deleting its
/// queue makes the broker cancel the subscription — and the worker resurrects, redeclaring the queue and
/// re-subscribing, so a command sent afterwards is handled again. Proves against a real broker the
/// self-healing that was only verified by hand before.
/// </summary>
[Trait("Category", "Integration")]
public sealed class ResurrectionTests : IClassFixture<RabbitMqFixture>
{
    private const string Exchange = "resurrection-tests";
    private const string Queue = "resurrection-tests.handler";

    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public ResurrectionTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>Deleting the queue cancels the consumer; the worker resurrects and keeps processing.</summary>
    [Fact]
    public async Task DeletingTheQueue_ResurrectsTheConsumer_AndKeepsProcessing()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();
        ConcurrentQueue<string> handled = new ConcurrentQueue<string>();

        HostApplicationBuilder builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(handled);
        builder.Services.AddBusContext(
            configuration,
            producer => producer.AddCommand<ResurrectionCommand>(Exchange),
            consumer => consumer.AddCommandHandler<ResurrectionCommand, ResurrectionCommandHandler>(Queue));

        using IHost host = builder.Build();
        await host.StartAsync(cancellationToken);

        try
        {
            await using IConnection admin = await Broker.ConnectAsync(configuration, cancellationToken);

            // before the interruption: a command is handled normally (proves the consumer is up)
            await SendAsync(host, "before", cancellationToken);
            await WaitForHandledAsync(handled, "before", cancellationToken);

            // the interruption: delete the consumer's queue, so the broker cancels the subscription
            await using (IChannel channel = await admin.CreateChannelAsync(cancellationToken: cancellationToken))
            {
                await channel.QueueDeleteAsync(
                    Queue,
                    ifUnused: false,
                    ifEmpty: false,
                    cancellationToken: cancellationToken);
            }

            // the worker heals itself: it redeclares the queue and re-subscribes a consumer
            await WaitForResurrectionAsync(admin, cancellationToken);

            // after the interruption: a freshly sent command is handled again
            await SendAsync(host, "after", cancellationToken);
            await WaitForHandledAsync(handled, "after", cancellationToken);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>Sends a command through a fresh scope's <see cref="IBus"/>.</summary>
    private static async Task SendAsync(
        IHost host,
        string payload,
        CancellationToken cancellationToken
    )
    {
        await using AsyncServiceScope scope = host.Services.CreateAsyncScope();

        await scope.ServiceProvider.GetRequiredService<IBus>().Send(
            new ResurrectionCommand(payload),
            cancellationToken);
    }

    /// <summary>Waits until the given payload has been recorded by the handler, failing after 30 seconds.</summary>
    private static async Task WaitForHandledAsync(
        ConcurrentQueue<string> handled,
        string payload,
        CancellationToken cancellationToken
    )
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);

        while (DateTime.UtcNow < deadline)
        {
            if (handled.Contains(payload))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
        }

        Assert.Fail($"'{payload}' was not handled within 30 seconds.");
    }

    /// <summary>
    /// Waits until the queue exists again with a consumer subscribed — the observable proof the worker
    /// resurrected (the first backoff step is five real seconds, so the timeout is generous).
    /// </summary>
    private static async Task WaitForResurrectionAsync(
        IConnection admin,
        CancellationToken cancellationToken
    )
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);

        while (DateTime.UtcNow < deadline)
        {
            await using IChannel channel = await admin.CreateChannelAsync(cancellationToken: cancellationToken);

            try
            {
                QueueDeclareOk declared = await channel.QueueDeclarePassiveAsync(Queue, cancellationToken);

                if (declared.ConsumerCount >= 1)
                {
                    return;
                }
            }
            catch (RabbitMQClientException)
            {
                // the queue is not back yet — the passive declare closed this channel; the next pass opens a fresh one.
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300), cancellationToken);
        }

        Assert.Fail("The consumer did not resurrect within 30 seconds.");
    }
}
