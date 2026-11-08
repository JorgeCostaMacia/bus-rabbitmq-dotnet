using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;

/// <summary>
/// The package's wiring: registers the Quartz-backed <see cref="RetryScheduler"/> behind the
/// <see cref="IRetryScheduler"/> the bus resolves — a singleton, since it is stateless and only wraps
/// the application's (singleton) <c>ISchedulerFactory</c>, so a delivery's scoped error handler safely
/// resolves the one shared instance to park its delayed retries. The retry job itself is not
/// registered: Quartz's dependency-injection job factory resolves it (and its bus producer) per fire.
/// </summary>
internal static class RetryInfrastructureContext
{
    /// <summary>Registers the Quartz-backed <see cref="IRetryScheduler"/> as a singleton.</summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    internal static IServiceCollection AddRetryInfrastructureContext(this IServiceCollection services)
    {
        services.AddSingleton<IRetryScheduler, RetryScheduler>();

        return services;
    }
}
