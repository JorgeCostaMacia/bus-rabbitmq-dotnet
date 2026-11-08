using System.Globalization;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.IntegrationTests;

/// <summary>
/// The shared infrastructure for the scheduled-retry suite: boots one ephemeral, plain-AMQP
/// <see cref="RabbitMqContainer"/> and one ephemeral <see cref="PostgreSqlContainer"/> (both from
/// pinned images), creates the Quartz ADO tables in Postgres so the persistent store has its schema,
/// and disposes both when the fixture tears down. Shared across the test class as an
/// <see cref="IClassFixture{TFixture}"/> so the pair starts once, not once per test.
/// </summary>
public sealed class RetryQuartzFixture : IAsyncLifetime
{
    private const string RabbitMqImage = "rabbitmq:4.0-management";
    private const string PostgresImage = "postgres:16";

    private readonly RabbitMqContainer _rabbitMq = new RabbitMqBuilder(RabbitMqImage)
        .Build();

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder(PostgresImage)
        .Build();

    /// <summary>The Npgsql connection string of the running Postgres container — the Quartz store points
    /// here.</summary>
    public string PostgresConnectionString => _postgres.GetConnectionString();

    /// <summary>Starts both containers, pulling the images on first use. The Quartz schema is provisioned by the store
    /// itself — see the test's <c>ProvisionSchema</c>.</summary>
    public async ValueTask InitializeAsync()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await Task.WhenAll(_rabbitMq.StartAsync(cancellationToken), _postgres.StartAsync(cancellationToken));
    }

    /// <summary>Stops and removes both containers.</summary>
    public async ValueTask DisposeAsync()
    {
        await _rabbitMq.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    /// <summary>
    /// Builds the bus configuration for the running broker's <c>Bus:Connection</c> section: the mapped
    /// plain-AMQP endpoint and the module's provisioned credentials, with <c>Ssl</c> forced
    /// <see langword="false"/> — the container speaks plain AMQP while the bus defaults to TLS on 5671,
    /// so the integration config must disable TLS and point at the mapped 5672.
    /// </summary>
    /// <returns>An in-memory configuration carrying the <c>Bus:Connection</c> keys.</returns>
    public IConfiguration BuildConfiguration()
    {
        Uri uri = new Uri(_rabbitMq.GetConnectionString());
        string[] userInfo = uri.UserInfo.Split(':');

        Dictionary<string, string?> settings = new Dictionary<string, string?>()
        {
            ["Bus:Connection:HostName"] = _rabbitMq.Hostname,
            ["Bus:Connection:UserName"] = userInfo[0],
            ["Bus:Connection:Password"] = userInfo[1],
            ["Bus:Connection:Ssl"] = "false",
            ["Bus:Connection:Port"] = _rabbitMq.GetMappedPublicPort(5672).ToString(CultureInfo.InvariantCulture)
        };

        return new ConfigurationBuilder()
            .AddInMemoryCollection(settings)
            .Build();
    }

    /// <summary>Counts the durable jobs parked in the Quartz store — a positive count is a retry parked in
    /// Postgres.</summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The number of rows in <c>qrtz_job_details</c>.</returns>
    public async Task<long> CountParkedJobs(CancellationToken cancellationToken)
    {
        await using NpgsqlConnection connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using NpgsqlCommand command = new NpgsqlCommand("SELECT count(*) FROM qrtz_job_details", connection);

        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }
}
