using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure;

/// <summary>
/// The parts of the connection wrapper that need no broker. Opening, re-opening and the client's
/// shutdown/recovery callbacks all require a real connection and belong to the integration suite; what
/// is pinned here is the readiness answer it gives before anything has ever connected, which is the one
/// the health check reads on a freshly started pod.
/// </summary>
public sealed class ConnectionTests
{
    [Fact]
    public void IsOpen_BeforeAnythingHasConnected_IsTrue()
    {
        // the connection is opened lazily on first use, so a process that has not produced or consumed
        // yet has no connection at all — and that must read as healthy. Answering false here would make
        // every pod fail its readiness probe until the first message moved, which for a consumer-only
        // service could be a long time.
        Connection connection = new Connection(new ConnectionFactory(), NullLogger.Instance);

        Assert.True(connection.IsOpen);
    }
}
