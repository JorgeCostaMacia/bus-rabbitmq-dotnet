using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz;

/// <summary>
/// The package's registration facade. It plugs the Quartz-backed delayed retry onto the Quartz
/// scheduler the application has already registered (its own <c>AddQuartz</c> with a persistent,
/// clustered store) — this package is agnostic to the store, provider and serialization. Only the
/// sending side registers here: the executing service(s) just reference this package and run Quartz,
/// whose dependency-injection job factory resolves the retry job (and its bus producer) on its own.
/// </summary>
public static class RetryContext
{
    /// <summary>
    /// Registers the Quartz-backed <see cref="IRetryScheduler"/> — the sending side, which parks a
    /// delayed retry as a one-shot job on the application's Quartz store. Registering it enables the
    /// positive intervals of the retry ladder. The application must have registered Quartz
    /// (<c>AddQuartz</c>) with a persistent store shared with the executing fleet.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddRetryContext(this IServiceCollection services)
        => services.AddRetryInfrastructureContext();
}
