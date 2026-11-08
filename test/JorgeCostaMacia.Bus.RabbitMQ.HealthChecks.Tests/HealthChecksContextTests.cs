using JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Infrastructure;
using JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Tests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using IConnection = JorgeCostaMacia.Bus.RabbitMQ.Domain.IConnection;

namespace JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Tests;

public sealed class HealthChecksContextTests
{
    private static ServiceProvider Provider(Action<IHealthChecksBuilder> add)
    {
        ServiceCollection services = new ServiceCollection();

        services.AddSingleton<IConnection>(new ConnectionFake());
        add(services.AddHealthChecks());

        return services.BuildServiceProvider();
    }

    private static HealthCheckRegistration Registration(ServiceProvider provider)
        => Assert.Single(provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations);

    [Fact]
    public void AddRabbitMQBus_WithoutArguments_RegistersTheCheckUnderItsDefaultName()
    {
        ServiceProvider provider = Provider(builder => builder.AddRabbitMQBus());

        HealthCheckRegistration registration = Registration(provider);
        Assert.Equal("bus-rabbitmq", registration.Name);
        Assert.Equal(HealthStatus.Unhealthy, registration.FailureStatus);
        Assert.Empty(registration.Tags);
    }

    [Fact]
    public void AddRabbitMQBus_WithANameStatusAndTags_RegistersThemOnTheCheck()
    {
        ServiceProvider provider = Provider(
            builder => builder.AddRabbitMQBus("orders-bus", HealthStatus.Degraded, new string[] { "ready" }));

        HealthCheckRegistration registration = Registration(provider);
        Assert.Equal("orders-bus", registration.Name);
        Assert.Equal(HealthStatus.Degraded, registration.FailureStatus);
        Assert.Equal("ready", Assert.Single(registration.Tags));
    }

    [Fact]
    public void AddRabbitMQBus_ResolvedThroughItsFactory_BuildsTheCheckOverTheBusConnection()
    {
        ServiceProvider provider = Provider(builder => builder.AddRabbitMQBus());

        Assert.IsType<BusHealthCheck>(Registration(provider).Factory(provider));
    }
}
