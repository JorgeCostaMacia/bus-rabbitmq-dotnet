using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Tests;

public sealed class RetryContextTests
{
    [Fact]
    public void AddRetryContext_RegistersTheQuartzRetryScheduler_AsSingleton()
    {
        ServiceCollection services = new ServiceCollection();

        services.AddRetryContext();

        ServiceDescriptor scheduler = Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(IRetryScheduler));
        Assert.Equal(typeof(RetryScheduler), scheduler.ImplementationType);
        Assert.Equal(ServiceLifetime.Singleton, scheduler.Lifetime);
    }
}
