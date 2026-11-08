namespace JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;

/// <summary>
/// Default connection settings a <see cref="ConnectionConfiguration"/> falls back to for values the
/// <c>Bus:Connection</c> section does not supply.
/// </summary>
public static class ConnectionConfigurationDefaults
{
    /// <summary>
    /// Whether TLS wraps the connection. Default: <c>true</c> — secure by default, matching the
    /// Kafka transport's SaslSsl default.
    /// </summary>
    public const bool Ssl = true;

    /// <summary>
    /// The AMQPS port. Default: <c>5671</c> — the only default port: the plain 5672 is never a
    /// fallback, so turning TLS off also requires supplying the port explicitly.
    /// </summary>
    public const int Port = 5671;

    /// <summary>Virtual host. Default: <c>/</c>.</summary>
    public const string VirtualHost = "/";

    /// <summary>Automatic connection/topology recovery. Default: <c>true</c> — the connection self-heals after a
    /// drop.</summary>
    public const bool AutomaticRecoveryEnabled = true;

    /// <summary>Client-provided connection name (shown in the RabbitMQ management UI). Default: the machine
    /// name.</summary>
    public static string ClientProvidedName => Environment.MachineName;
}
