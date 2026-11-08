using JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Infrastructure;
using JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Tests.Support;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Tests.Infrastructure;

public sealed class BusHealthCheckTests
{
    private readonly ConnectionFake _connection = new ConnectionFake();

    private static HealthCheckContext Context(
        IHealthCheck check,
        HealthStatus? failureStatus = null
    )
        => new HealthCheckContext()
        {
            Registration = new HealthCheckRegistration("bus-rabbitmq", check, failureStatus, tags: null)
        };

    [Fact]
    public async Task CheckHealthAsync_ConnectionOpen_ReportsHealthy()
    {
        BusHealthCheck check = new BusHealthCheck(_connection);

        HealthCheckResult result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Equal("The RabbitMQ connection is open.", result.Description);
    }

    [Fact]
    public async Task CheckHealthAsync_ConnectionDown_ReportsUnhealthyByDefault()
    {
        _connection.IsOpen = false;
        BusHealthCheck check = new BusHealthCheck(_connection);

        HealthCheckResult result = await check.CheckHealthAsync(Context(check), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal("The RabbitMQ connection is down; automatic recovery keeps retrying.", result.Description);
    }

    [Fact]
    public async Task CheckHealthAsync_ConnectionDown_HonorsTheRegistrationFailureStatus()
    {
        _connection.IsOpen = false;
        BusHealthCheck check = new BusHealthCheck(_connection);

        HealthCheckResult result = await check.CheckHealthAsync(
            Context(check, HealthStatus.Degraded),
            TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Degraded, result.Status);
    }
}
