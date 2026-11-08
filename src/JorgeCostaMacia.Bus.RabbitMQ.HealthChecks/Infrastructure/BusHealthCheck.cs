using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.Infrastructure;

/// <summary>
/// The health check over the bus's shared RabbitMQ connection — no probe connection, no extra I/O:
/// it reads the state of the one long-lived connection the transport already owns. Healthy while the
/// connection is open (a connection never opened yet counts as open — the bus opens it lazily on
/// first use); when it has dropped, it reports the registration's failure status while automatic
/// recovery keeps retrying. Internal: registered through
/// <see cref="HealthChecksContext.AddRabbitMQBus"/>, whose factory resolves the internal connection.
/// </summary>
internal sealed class BusHealthCheck : IHealthCheck
{
    private readonly Domain.IConnection _connection;

    /// <summary>Creates the check over the bus's shared connection.</summary>
    /// <param name="connection">The bus's shared RabbitMQ connection.</param>
    public BusHealthCheck(Domain.IConnection connection)
    {
        _connection = connection;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
        => Task.FromResult(_connection.IsOpen
            ? HealthCheckResult.Healthy("The RabbitMQ connection is open.")
            : new HealthCheckResult(
                context.Registration.FailureStatus,
                "The RabbitMQ connection is down; automatic recovery keeps retrying."));
}
