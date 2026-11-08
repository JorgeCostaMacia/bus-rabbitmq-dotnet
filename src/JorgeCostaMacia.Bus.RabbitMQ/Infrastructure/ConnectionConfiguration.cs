using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// The connection configuration, bound from the <c>Bus:Connection</c> section: the single RabbitMQ
/// connection shared by the producing and consuming sides. Unset values fall back to
/// <see cref="ConnectionConfigurationDefaults"/> when composing the <see cref="ConnectionFactory"/>.
/// </summary>
public sealed record ConnectionConfiguration
{
    /// <summary>The RabbitMQ host name. Required.</summary>
    public required string HostName { get; init; }

    /// <summary>The user name. Required.</summary>
    public required string UserName { get; init; }

    /// <summary>The password. Required.</summary>
    public required string Password { get; init; }

    /// <summary>Whether TLS wraps the connection, or <see langword="null"/> for the default (true — secure by
    /// default).</summary>
    public bool? Ssl { get; init; }

    /// <summary>The port, or <see langword="null"/> for the default (5671, AMQPS — going plain requires an explicit
    /// port).</summary>
    public int? Port { get; init; }

    /// <summary>The virtual host, or <see langword="null"/> for the default (<c>/</c>).</summary>
    public string? VirtualHost { get; init; }

    /// <summary>The client-provided connection name, or <see langword="null"/> for the default (machine
    /// name).</summary>
    public string? ClientProvidedName { get; init; }

    /// <summary>Automatic recovery, or <see langword="null"/> for the default (true).</summary>
    public bool? AutomaticRecoveryEnabled { get; init; }

    /// <summary>The RabbitMQ connection factory — supplied values, defaults for the rest; TLS carries the host name for
    /// the certificate match.</summary>
    public ConnectionFactory ConnectionFactory
    {
        get
        {
            bool ssl = Ssl ?? ConnectionConfigurationDefaults.Ssl;

            return new ConnectionFactory
            {
                HostName = HostName,
                UserName = UserName,
                Password = Password,
                Ssl = new SslOption { Enabled = ssl, ServerName = HostName },
                Port = Port ?? ConnectionConfigurationDefaults.Port,
                VirtualHost = VirtualHost ?? ConnectionConfigurationDefaults.VirtualHost,
                ClientProvidedName = ClientProvidedName ?? ConnectionConfigurationDefaults.ClientProvidedName,
                AutomaticRecoveryEnabled = AutomaticRecoveryEnabled
                    ?? ConnectionConfigurationDefaults.AutomaticRecoveryEnabled
            };
        }
    }
}
