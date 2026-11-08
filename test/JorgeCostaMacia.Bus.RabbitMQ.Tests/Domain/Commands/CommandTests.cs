using System.Collections.Immutable;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Domain.Commands;

public sealed class CommandTests
{
    [Fact]
    public void Serialize_ACommandWithEveryTraceField_RoundTripsThemAll()
    {
        TestCommand command = new TestCommand(
            "pepe",
            Guid.NewGuid(),
            Guid.NewGuid(),
            new DateTime(2026, 7, 3, 10, 0, 0, DateTimeKind.Utc),
            ImmutableList.Create("g1", "g2"));

        TestCommand roundTripped = JsonSerializer.Deserialize<TestCommand>(
            JsonSerializer.SerializeToUtf8Bytes(command))!;

        Assert.Equal(command.Name, roundTripped.Name);
        Assert.Equal(command.AggregateId, roundTripped.AggregateId);
        Assert.Equal(command.AggregateCorrelationId, roundTripped.AggregateCorrelationId);
        Assert.Equal(command.AggregateOccurredAt, roundTripped.AggregateOccurredAt);
        Assert.Equal(command.AggregateConsumers, roundTripped.AggregateConsumers);
    }

    [Fact]
    public void Constructor_WithOnlyAName_GeneratesTheTrace()
    {
        DateTime before = DateTime.UtcNow;

        TestCommand command = new TestCommand("pepe");

        Assert.NotEqual(Guid.Empty, command.AggregateId);
        Assert.Equal(command.AggregateId, command.AggregateCorrelationId);
        Assert.InRange(command.AggregateOccurredAt, before, DateTime.UtcNow);
        Assert.Empty(command.AggregateConsumers);
    }

    [Fact]
    public void Constructor_WithEveryTraceValue_KeepsThem()
    {
        Guid id = Guid.NewGuid();
        Guid correlation = Guid.NewGuid();
        DateTime occurredAt = new DateTime(2026, 7, 3, 10, 0, 0, DateTimeKind.Utc);

        TestCommand command = new TestCommand(
            "pepe",
            id,
            correlation,
            occurredAt,
            ImmutableList.Create("g1"));

        Assert.Equal(id, command.AggregateId);
        Assert.Equal(correlation, command.AggregateCorrelationId);
        Assert.Equal(occurredAt, command.AggregateOccurredAt);
        Assert.Equal(new string[] { "g1" }, command.AggregateConsumers);
    }
}
