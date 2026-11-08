using JorgeCostaMacia.Bus.RabbitMQ.Domain;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>
/// In-memory outbound gate capturing every published message (exchange, routing key, body and headers),
/// with an optional failure to throw — enough surface for the bus and the consumer failure policy.
/// </summary>
internal sealed class ProducerFake : IProducer
{
    /// <summary>The publishes handed to <see cref="Produce"/>, in order.</summary>
    public List<(
        string Exchange,
        string RoutingKey,
        byte[] Body,
        IReadOnlyDictionary<string, string> Headers)> Produced
    { get; }
        = new List<(string Exchange, string RoutingKey, byte[] Body, IReadOnlyDictionary<string, string> Headers)>();

    /// <summary>An exception to fail every publish with, or <see langword="null"/> to succeed.</summary>
    public Exception? Failure { get; set; }

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

    public Task Park(
        string queue,
        ReadOnlyMemory<byte> body,
        IReadOnlyDictionary<string, string> headers,
        CancellationToken cancellationToken = default
    ) => Produce(string.Empty, queue, body, headers, cancellationToken);
}
