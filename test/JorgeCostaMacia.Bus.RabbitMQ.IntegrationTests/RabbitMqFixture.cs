using System.Globalization;
using Microsoft.Extensions.Configuration;
using Testcontainers.RabbitMq;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests;

/// <summary>
/// The shared RabbitMQ broker for the integration suite: boots one ephemeral, plain-AMQP
/// <see cref="RabbitMqContainer"/> from a pinned image, exposes its mapped endpoint, and disposes
/// it (stop + remove) when the fixture tears down. Shared across a test class as an
/// <see cref="IClassFixture{TFixture}"/> so the broker starts once, not once per test.
/// </summary>
public sealed class RabbitMqFixture : IAsyncLifetime
{
    private const string RabbitMqImage = "rabbitmq:4.0-management";

    private readonly RabbitMqContainer _container = new RabbitMqBuilder(RabbitMqImage)
        .Build();

    /// <summary>Starts the container, pulling the image on first use.</summary>
    public ValueTask InitializeAsync()
        => new ValueTask(_container.StartAsync(TestContext.Current.CancellationToken));

    /// <summary>Stops and removes the container.</summary>
    public ValueTask DisposeAsync()
        => _container.DisposeAsync();

    /// <summary>
    /// Freezes the broker (Docker pause) so it stops answering while keeping its mapped port — the chaos
    /// tests use this to simulate a broker outage mid-load without the port remapping a stop/start would
    /// cause. Pair every pause with an <see cref="UnpauseAsync"/>.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public Task PauseAsync(CancellationToken cancellationToken)
        => _container.PauseAsync(cancellationToken);

    /// <summary>Thaws the broker (Docker unpause) so it answers again on the same mapped port.</summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    public Task UnpauseAsync(CancellationToken cancellationToken)
        => _container.UnpauseAsync(cancellationToken);

    /// <summary>
    /// Builds the bus configuration for the running container's <c>Bus:Connection</c> section: the
    /// mapped plain-AMQP endpoint and the module's provisioned credentials, with <c>Ssl</c> forced
    /// <see langword="false"/> — the container speaks plain AMQP while the bus defaults to TLS on
    /// 5671, so the integration config must disable TLS and point at the mapped 5672.
    /// </summary>
    /// <returns>An in-memory configuration carrying the <c>Bus:Connection</c> keys.</returns>
    public IConfiguration BuildConfiguration()
    {
        // GetConnectionString() reflects whatever user/password the module provisioned (a custom,
        // non-loopback-restricted user — never the loopback-only 'guest'), so parse it rather than
        // hard-coding credentials.
        Uri uri = new Uri(_container.GetConnectionString());
        string[] userInfo = uri.UserInfo.Split(':');

        Dictionary<string, string?> settings = new Dictionary<string, string?>()
        {
            ["Bus:Connection:HostName"] = _container.Hostname,
            ["Bus:Connection:UserName"] = userInfo[0],
            ["Bus:Connection:Password"] = userInfo[1],
            ["Bus:Connection:Ssl"] = "false",
            ["Bus:Connection:Port"] = _container.GetMappedPublicPort(5672).ToString(CultureInfo.InvariantCulture)
        };

        return new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();
    }
}
