using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers;

/// <summary>
/// The inbound gate over a RabbitMQ <see cref="IChannel"/> — wraps the channel a worker opened and
/// exposes only what the <see cref="ConsumerWorker{TContext, THandler}"/> needs (declare, consume,
/// ack, nack), owning it and disposing it with the worker. One per worker; a channel is not safe for
/// concurrent use, but a single worker drives it one delivery at a time.
/// </summary>
internal sealed class ConsumerChannel : IConsumerChannel
{
    private readonly IChannel _channel;

    /// <summary>Wraps the channel the worker opened.</summary>
    /// <param name="channel">The RabbitMQ channel this gate owns.</param>
    public ConsumerChannel(IChannel channel)
    {
        _channel = channel;
    }

    /// <inheritdoc />
    public bool IsOpen => _channel.IsOpen;

    /// <inheritdoc />
    public async Task DeclareAsync(
        string exchange,
        string exchangeType,
        string queue,
        ushort prefetchCount,
        CancellationToken cancellationToken = default
    )
    {
        await _channel.ExchangeDeclareAsync(
            exchange,
            exchangeType,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);
        await _channel.QueueDeclareAsync(
            queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);
        await _channel.QueueBindAsync(queue, exchange, routingKey: string.Empty, cancellationToken: cancellationToken);

        await _channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: prefetchCount,
            global: false,
            cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task ConsumeAsync(
        string queue,
        Func<BasicDeliverEventArgs, Task> onReceived,
        Func<ShutdownEventArgs?, Task> onClosed,
        CancellationToken cancellationToken = default
    )
    {
        AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += (_, args) => onReceived(args);
        consumer.UnregisteredAsync += (_, _) => onClosed(null);

        _channel.ChannelShutdownAsync += (_, args) => onClosed(args);

        await _channel.BasicConsumeAsync(queue, autoAck: false, consumer, cancellationToken);
    }

    /// <inheritdoc />
    public async Task AckAsync(
        ulong deliveryTag,
        CancellationToken cancellationToken = default
    ) => await _channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);

    /// <inheritdoc />
    public async Task NackAsync(
        ulong deliveryTag,
        bool requeue,
        CancellationToken cancellationToken = default
    ) => await _channel.BasicNackAsync(deliveryTag, multiple: false, requeue, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _channel.DisposeAsync();
}
