using System.Globalization;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// Raw RabbitMQ.Client access for the failure-lane tests: opens a bare connection straight to the
/// container from the same <c>Bus:Connection</c> config the fixture builds, and reads back what the
/// bus parked. It deliberately bypasses the bus so the assertions observe the broker's own state —
/// the message truly sitting in <c>{queue}.error</c> / <c>{queue}.fault</c>, the exception the broker
/// actually raises for an unroutable mandatory publish — rather than trusting the bus to report on
/// itself. Every channel it hands out has publisher confirmations (with tracking) on, matching the
/// bus's own channels, so a mandatory publish completes only once the broker has accepted or returned.
/// </summary>
internal static class Broker
{
    /// <summary>Builds the connection factory for the running container from the bus's <c>Bus:Connection</c> config —
    /// plain AMQP on the mapped port, the provisioned credentials, confirms tracked on every channel it
    /// opens.</summary>
    /// <param name="configuration">The configuration the fixture built, carrying the <c>Bus:Connection</c>
    /// keys.</param>
    /// <returns>A connection factory pointed at the container.</returns>
    public static ConnectionFactory Factory(IConfiguration configuration)
    {
        string host = configuration["Bus:Connection:HostName"]!;
        string user = configuration["Bus:Connection:UserName"]!;
        string password = configuration["Bus:Connection:Password"]!;
        int port = int.Parse(configuration["Bus:Connection:Port"]!, CultureInfo.InvariantCulture);

        return new ConnectionFactory
        {
            HostName = host,
            UserName = user,
            Password = password,
            Port = port,
            Ssl = new SslOption { Enabled = false }
        };
    }

    /// <summary>Opens a bare connection to the container.</summary>
    /// <param name="configuration">The bus's <c>Bus:Connection</c> configuration.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An open connection the caller disposes.</returns>
    public static Task<IConnection> ConnectAsync(
        IConfiguration configuration,
        CancellationToken cancellationToken
    ) => Factory(configuration).CreateConnectionAsync(cancellationToken);

    /// <summary>Opens a channel with publisher confirmations and tracking on — the same options the bus's producer
    /// uses, so a mandatory publish surfaces an unroutable message as a thrown exception rather than a silent
    /// drop.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A confirm-tracking channel the caller disposes.</returns>
    public static Task<IChannel> ConfirmChannelAsync(
        IConnection connection,
        CancellationToken cancellationToken
    )
        => connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            cancellationToken);

    /// <summary>
    /// Waits for a message to land in a park queue born lazily on first park, then gets it back: each
    /// pass opens a fresh channel and passively declares the queue — a passive declare on a queue that
    /// does not exist yet raises a broker exception and closes only that channel, so the next pass
    /// retries after a short delay — and once the queue exists with a message, it is fetched (auto-acked)
    /// and returned. Returns <see langword="null"/> if nothing is parked within the timeout.
    /// </summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="queue">The park queue to poll (e.g. <c>{queue}.error</c>).</param>
    /// <param name="timeout">How long to wait for the lazily-born queue to hold a message.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The parked message, or <see langword="null"/> if none arrived in time.</returns>
    public static async Task<BasicGetResult?> WaitForParkedAsync(
        IConnection connection,
        string queue,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        DateTime deadline = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < deadline)
        {
            IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            try
            {
                QueueDeclareOk declared = await channel.QueueDeclarePassiveAsync(queue, cancellationToken);

                if (declared.MessageCount > 0)
                {
                    BasicGetResult? result = await channel.BasicGetAsync(queue, autoAck: true, cancellationToken);

                    if (result is not null)
                    {
                        return result;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (RabbitMQClientException)
            {
                // The park queue is not born yet — the passive declare closed this channel; the next pass opens a fresh
                // one.
            }
            finally
            {
                await channel.DisposeAsync();
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
        }

        return null;
    }

    /// <summary>Reads a header's text from a parked delivery — the bus writes the <c>jcm-*</c> headers as AMQP longstr
    /// (a byte array on the wire), so a byte array is decoded as UTF-8, and anything else falls back to its
    /// text.</summary>
    /// <param name="result">The parked delivery.</param>
    /// <param name="key">The header key.</param>
    /// <returns>The header text, or <see langword="null"/> when the header is absent.</returns>
    public static string? Header(
        BasicGetResult result,
        string key
    )
    {
        if (result.BasicProperties.Headers is null
            || !result.BasicProperties.Headers.TryGetValue(key, out object? value)
            || value is null)
        {
            return null;
        }

        return value is byte[] bytes
            ? System.Text.Encoding.UTF8.GetString(bytes)
            : value.ToString();
    }
}
