using System.Collections.Immutable;
using System.Text.Json.Serialization;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Events;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;

/// <summary>Command used across the tests — the serializer's constructor on top, the convenient one below.</summary>
internal sealed record TestCommand : Command
{
    public string Name { get; init; }

    [JsonConstructor]
    public TestCommand(
        Guid aggregateId,
        Guid aggregateCorrelationId,
        DateTime aggregateOccurredAt,
        ImmutableList<string> aggregateConsumers,
        string name
    ) : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
    {
        Name = name;
    }

    public TestCommand(
        string name,
        Guid? aggregateId = null,
        Guid? aggregateCorrelationId = null,
        DateTime? aggregateOccurredAt = null,
        IEnumerable<string>? aggregateConsumers = null
    ) : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
    {
        Name = name;
    }
}

/// <summary>Event used across the tests — the serializer's constructor on top, the convenient one below.</summary>
internal sealed record TestEvent : Event
{
    public string Name { get; init; }

    [JsonConstructor]
    public TestEvent(
        Guid aggregateId,
        Guid aggregateCorrelationId,
        DateTime aggregateOccurredAt,
        ImmutableList<string> aggregateConsumers,
        string name
    ) : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
    {
        Name = name;
    }

    public TestEvent(
        string name,
        Guid? aggregateId = null,
        Guid? aggregateCorrelationId = null,
        DateTime? aggregateOccurredAt = null,
        IEnumerable<string>? aggregateConsumers = null
    ) : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
    {
        Name = name;
    }
}

/// <summary>No-op handler used to exercise the configurator registrations.</summary>
internal sealed class TestCommandHandler : CommandHandler<TestCommand>
{
    public override Task Handle(
        CommandContext<TestCommand> context,
        CancellationToken cancellationToken = default
    ) => Task.CompletedTask;
}

/// <summary>No-op subscriber used to exercise the configurator registrations.</summary>
internal sealed class TestEventSubscriber : EventSubscriber<TestEvent>
{
    public override Task Handle(
        EventContext<TestEvent> context,
        CancellationToken cancellationToken = default
    ) => Task.CompletedTask;
}
