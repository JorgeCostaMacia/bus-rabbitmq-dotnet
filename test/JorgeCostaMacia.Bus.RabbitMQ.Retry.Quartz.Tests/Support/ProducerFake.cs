using JorgeCostaMacia.Bus.RabbitMQ.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.Tests.Support;

/// <summary>In-memory outbound gate capturing every produced message — the seam the retry job re-produces
/// through.</summary>
internal sealed class ProducerFake : IProducer
{
    public List<(
        string Exchange,
        string RoutingKey,
        byte[] Body,
        IReadOnlyDictionary<string, string> Headers)> Produced
    { get; }
        = new List<(string Exchange, string RoutingKey, byte[] Body, IReadOnlyDictionary<string, string> Headers)>();

    /// <summary>When set, every produce throws it — the broker-down seam.</summary>
    public Exception? Failure { get; set; }

    public Task Park(
        string queue,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        CancellationToken cancellationToken = default
    ) => throw new NotImplementedException();

    public Task Produce(
        string exchange,
        string routingKey,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        CancellationToken cancellationToken = default
    )
    {
        if (Failure is not null)
        {
            throw Failure;
        }

        Produced.Add((exchange, routingKey, body.ToArray(), headers));

        return Task.CompletedTask;
    }
}
