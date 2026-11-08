using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace JorgeCostaMacia.Bus.RabbitMQ;

/// <summary>
/// The package's registration facade: registers the bus over the <c>Bus:Connection</c> configuration
/// section. The producer lambda maps the messages this service sends/publishes to their exchanges;
/// the optional consumer lambda registers its handlers — omit it for a send-only service.
/// </summary>
public static class BusContext
{
    /// <summary>Registers the bus over the application configuration.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Connection</c> section.</param>
    /// <param name="producer">Maps the messages this service sends/publishes to their exchanges.</param>
    /// <param name="consumer">Registers this service's handlers and subscribers, or <see langword="null"/> for a
    /// send-only service.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    public static IServiceCollection AddBusContext(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<ProducerConfigurator> producer,
        Action<ConsumerConfigurator>? consumer = null
    ) => services.AddBusInfrastructureContext(configuration, producer, consumer);
}
