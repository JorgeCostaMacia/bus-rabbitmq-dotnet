using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;
using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Producers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using IBus = JorgeCostaMacia.Bus.RabbitMQ.Domain.IBus;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The package's wiring: the single RabbitMQ connection is a container-owned singleton built from the
/// <c>Bus:Connection</c> section; the outbound gate (<see cref="IProducer"/>) is a singleton holding
/// one confirmation-enabled channel per destination exchange (concurrent publishes pipeline on it),
/// and the bus (<see cref="IBus"/>) stays scoped. The send side maps messages to exchanges through the
/// <see cref="ProducerConfigurator"/>; the optional consume side registers its handlers and their
/// hosted consumers through the <see cref="ConsumerConfigurator"/> (omit it for a send-only service).
/// </summary>
internal static class BusInfrastructureContext
{
    private const string ConnectionSection = "Bus:Connection";

    /// <summary>Registers the bus over the application configuration.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">The application configuration carrying the <c>Bus:Connection</c> section.</param>
    /// <param name="producer">Maps the messages this service sends/publishes to their exchanges.</param>
    /// <param name="consumer">Registers this service's handlers and subscribers, or <see langword="null"/> for a
    /// send-only service.</param>
    /// <returns>The same service collection, to allow method chaining.</returns>
    internal static IServiceCollection AddBusInfrastructureContext(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<ProducerConfigurator> producer,
        Action<ConsumerConfigurator>? consumer = null
    )
    {
        ConnectionConfiguration connectionConfiguration = CreateConnectionConfiguration(configuration);

        ProducerConfigurator producerConfigurator = new ProducerConfigurator();

        producer(producerConfigurator);

        services.AddSingleton<IConnection>(
            provider => new Connection(
                connectionConfiguration.ConnectionFactory,
                provider.GetRequiredService<ILoggerFactory>().CreateLogger(RabbitLogger.Category)));
        services.AddSingleton<IConsumerChannelFactory>(
            provider => new Consumers.ConsumerChannelFactory(provider.GetRequiredService<IConnection>()));
        services.AddSingleton<IProducer, Producer>();
        services.AddScoped<IBus>(
            provider => new Bus(provider.GetRequiredService<IProducer>(), producerConfigurator.Messages));

        // registered before the consumers so the producer's exchanges exist before anything binds
        // to them — and a send-only service creates its own topology with no consumer involved.
        services.AddHostedService(
            provider => new TopologyWorker(provider.GetRequiredService<IConnection>(), producerConfigurator.Exchanges));

        if (consumer is not null)
        {
            ConsumerConfigurator consumerConfigurator = new ConsumerConfigurator(
                services,
                producerConfigurator.Messages);

            consumer(consumerConfigurator);
        }

        return services;
    }

    /// <summary>Binds and validates the <c>Bus:Connection</c> section onto a
    /// <see cref="ConnectionConfiguration"/>.</summary>
    private static ConnectionConfiguration CreateConnectionConfiguration(IConfiguration configuration)
    {
        ConnectionConfiguration connectionConfiguration
            = configuration.GetSection(ConnectionSection).Get<ConnectionConfiguration>()
            ?? throw new InvalidOperationException($"'{ConnectionSection}' is null.");

        if (string.IsNullOrWhiteSpace(connectionConfiguration.HostName))
        {
            throw new InvalidOperationException(
                $"'{ConnectionSection}:{nameof(connectionConfiguration.HostName)}' is null.");
        }

        if (string.IsNullOrWhiteSpace(connectionConfiguration.UserName))
        {
            throw new InvalidOperationException(
                $"'{ConnectionSection}:{nameof(connectionConfiguration.UserName)}' is null.");
        }

        if (string.IsNullOrWhiteSpace(connectionConfiguration.Password))
        {
            throw new InvalidOperationException(
                $"'{ConnectionSection}:{nameof(connectionConfiguration.Password)}' is null.");
        }

        return connectionConfiguration;
    }
}
