namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// Opens an <see cref="IConsumerChannel"/> on the shared connection — one per worker, on start. The
/// seam that lets a worker take its channel from the container (the real one over the connection) or,
/// in a test, from a fake, so the delivery orchestration runs without a live broker.
/// </summary>
internal interface IConsumerChannelFactory
{
    /// <summary>Opens a new consumer channel on the shared connection.</summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The worker's inbound gate.</returns>
    Task<IConsumerChannel> CreateAsync(CancellationToken cancellationToken = default);
}
