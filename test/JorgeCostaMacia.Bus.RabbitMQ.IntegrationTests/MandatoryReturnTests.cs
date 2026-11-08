using System.Text;
using JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// Settles the open question of how the real broker signals a failed <c>Producer.Park</c>: the park
/// publishes with <c>mandatory: true</c> on a confirm-tracking channel, so an unroutable park must not
/// be silently dropped — it must throw. This drives exactly that against the live broker and captures
/// the actual exception type, then verifies the bus's error handler would classify it correctly.
/// <para>
/// Discovered against RabbitMQ.Client 7.2.1 + RabbitMQ 4.0: an unroutable mandatory publish on a
/// confirm-tracking channel throws <see cref="PublishReturnException"/> (a <c>basic.return</c>), whose
/// inheritance is <c>PublishReturnException : PublishException : RabbitMQClientException</c>. Because it
/// <b>is</b> a <see cref="RabbitMQClientException"/>, a failed park lands in the error handler's
/// <c>catch (RabbitMQClientException)</c> lane → <c>ErrorResult.Unhandled</c> (nack / redeliver), never
/// the <c>catch (Exception)</c> → <c>Faulted</c> branch. So the current classification is correct — no
/// defect — and this test guards that: a future client that reparented the type below
/// <see cref="RabbitMQClientException"/> would fail here.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class MandatoryReturnTests : IClassFixture<RabbitMqFixture>
{
    private readonly RabbitMqFixture _fixture;

    /// <summary>Takes the shared broker fixture.</summary>
    /// <param name="fixture">The running RabbitMQ container.</param>
    public MandatoryReturnTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    /// <summary>An unroutable mandatory publish throws <see cref="PublishReturnException"/>, which the error handler
    /// classifies as a <see cref="RabbitMQClientException"/> (the <c>Unhandled</c> lane, not <c>Faulted</c>).</summary>
    [Fact]
    public async Task MandatoryPublish_ThatIsUnroutable_ThrowsARabbitMQClientException()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        IConfiguration configuration = _fixture.BuildConfiguration();

        await using IConnection connection = await Broker.ConnectAsync(configuration, cancellationToken);
        await using IChannel channel = await Broker.ConfirmChannelAsync(connection, cancellationToken);

        // The default exchange routes by queue name; a queue that does not exist is unroutable, so a
        // mandatory publish is returned by the broker — the same failure mode a Producer.Park would hit
        // if its target queue could not be routed to.
        string unroutable = "no-such-queue-" + Guid.NewGuid().ToString("N");
        byte[] body = Encoding.UTF8.GetBytes("unroutable");

        Exception caught = await Assert.ThrowsAnyAsync<Exception>(
            () => channel.BasicPublishAsync(
                string.Empty,
                routingKey: unroutable,
                mandatory: true,
                body: body,
                cancellationToken: cancellationToken).AsTask());

        // The discovered type: a basic.return surfaced as a PublishReturnException.
        PublishReturnException returned = Assert.IsType<PublishReturnException>(caught);
        Assert.True(returned.IsReturn, "The publish exception was expected to be a basic.return.");

        // The classification the bus depends on: it IS a RabbitMQClientException, so the error handler's
        // catch (RabbitMQClientException) → ErrorResult.Unhandled lane catches a failed park — never the
        // catch (Exception) → ErrorResult.Faulted branch that would misroute it to the fault handler.
        Assert.IsAssignableFrom<RabbitMQClientException>(caught);
    }
}
