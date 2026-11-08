using JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace JorgeCostaMacia.Bus.RabbitMQ.HealthChecks;

/// <summary>
/// The package's registration facade: plugs the bus's health check onto ASP.NET Core's health-check
/// pipeline. The check reports whether the bus's shared broker connection is open — call it after
/// <c>AddBusContext</c>, which registers the connection it reads.
/// </summary>
public static class HealthChecksContext
{
    /// <summary>
    /// Registers the RabbitMQ bus health check: healthy while the bus's shared connection is open (a
    /// connection never opened yet counts as open — it is opened lazily on first use), the failure
    /// status while it is down and automatic recovery keeps retrying. Tag it (typically
    /// <c>"ready"</c>) to pick which health endpoints run it.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check's name, <c>bus-rabbitmq</c> by default.</param>
    /// <param name="failureStatus">The status reported when the connection is down, or <see langword="null"/> for
    /// <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">The tags the health endpoints filter on (typically <c>"ready"</c> for readiness), or
    /// <see langword="null"/> for none.</param>
    /// <returns>The same builder, to allow method chaining.</returns>
    public static IHealthChecksBuilder AddRabbitMQBus(
        this IHealthChecksBuilder builder,
        string name = "bus-rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null
    )
        => builder.Add(
            new HealthCheckRegistration(
                name,
                provider => new BusHealthCheck(provider.GetRequiredService<Domain.IConnection>()),
                failureStatus,
                tags));
}
