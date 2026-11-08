using JorgeCostaMacia.Bus.RabbitMQ.Infrastructure;
using RabbitMQ.Client;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure;

public sealed class ConnectionConfigurationTests
{
    [Fact]
    public void ConnectionFactory_UnsetValues_FallBackToTheDefaults()
    {
        ConnectionFactory factory = new ConnectionConfiguration
        {
            HostName = "bus",
            UserName = "user",
            Password = "pass"
        }.ConnectionFactory;

        Assert.Equal("bus", factory.HostName);
        Assert.Equal("user", factory.UserName);
        Assert.Equal("pass", factory.Password);
        Assert.True(factory.Ssl.Enabled);
        Assert.Equal("bus", factory.Ssl.ServerName);
        Assert.Equal(ConnectionConfigurationDefaults.Port, factory.Port);
        Assert.Equal(ConnectionConfigurationDefaults.VirtualHost, factory.VirtualHost);
        Assert.Equal(Environment.MachineName, factory.ClientProvidedName);
        Assert.Equal(ConnectionConfigurationDefaults.AutomaticRecoveryEnabled, factory.AutomaticRecoveryEnabled);
    }

    [Fact]
    public void ConnectionFactory_SslOff_KeepsTheSecurePortDefault()
    {
        // the plain 5672 is never a fallback: going plain is a fully explicit act — Ssl off AND the
        // port supplied; forgetting the port fails loudly against 5671 instead of silently degrading.
        ConnectionFactory factory = new ConnectionConfiguration
        {
            HostName = "bus",
            UserName = "user",
            Password = "pass",
            Ssl = false
        }.ConnectionFactory;

        Assert.False(factory.Ssl.Enabled);
        Assert.Equal(ConnectionConfigurationDefaults.Port, factory.Port);
    }

    [Fact]
    public void ConnectionFactory_SuppliedValues_Win()
    {
        ConnectionFactory factory = new ConnectionConfiguration
        {
            HostName = "bus",
            UserName = "user",
            Password = "pass",
            Port = 5673,
            VirtualHost = "vh",
            ClientProvidedName = "custom",
            AutomaticRecoveryEnabled = false
        }.ConnectionFactory;

        Assert.Equal(5673, factory.Port);
        Assert.Equal("vh", factory.VirtualHost);
        Assert.Equal("custom", factory.ClientProvidedName);
        Assert.False(factory.AutomaticRecoveryEnabled);
    }
}
