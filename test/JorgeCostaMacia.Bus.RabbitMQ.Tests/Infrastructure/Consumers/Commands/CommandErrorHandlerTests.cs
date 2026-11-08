using System.Collections.Immutable;
using System.Text.Json;
using JorgeCostaMacia.Bus.RabbitMQ.Domain;
using JorgeCostaMacia.Bus.RabbitMQ.Domain.Commands.Errors;
using JorgeCostaMacia.Bus.RabbitMQ.Tests.Support;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client.Exceptions;
using ErrorHandler = JorgeCostaMacia.Bus.RabbitMQ.Infrastructure.Consumers.Commands.CommandErrorHandler<
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.TestCommand,
    JorgeCostaMacia.Bus.RabbitMQ.Tests.Support.RecordingCommandHandler>;

namespace JorgeCostaMacia.Bus.RabbitMQ.Tests.Infrastructure.Consumers.Commands;

public sealed class CommandErrorHandlerTests
{
    private class BaseFailure : Exception;

    private sealed class DerivedFailure : BaseFailure;

    private sealed class BrokerFailure : RabbitMQClientException
    {
        public BrokerFailure() : base("broker down") { }
    }

    private readonly ProducerFake _producer = new ProducerFake();
    private readonly RetrySchedulerFake _scheduler = new RetrySchedulerFake();

    private ErrorHandler CommandError(
        ImmutableList<TimeSpan>? intervals = null,
        ImmutableList<Type>? excludes = null,
        bool scheduler = true,
        ILogger? logger = null
    )
        => new ErrorHandler(
            _producer,
            scheduler ? _scheduler : null,
            logger ?? NullLogger.Instance,
            Deliveries.Exchange,
            Deliveries.Queue,
            intervals ?? ImmutableList<TimeSpan>.Empty,
            excludes ?? ImmutableList<Type>.Empty);

    [Fact]
    public async Task NoLadder_ParksToErrorQueue()
    {
        CommandErrorContext<TestCommand> context = new CommandErrorContext<TestCommand>(
            new TestCommand("pepe"),
            Deliveries.Transport(),
            new InvalidOperationException("boom"));

        ErrorHandler sut = CommandError();
        await sut.Handle(context, TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string exchange, string routingKey, byte[] body, IReadOnlyDictionary<string, string> headers) = Assert.Single(
            _producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.Queue}.error", routingKey);

        JsonElement parked = JsonSerializer.Deserialize<JsonElement>(body);
        Assert.Equal(
            typeof(InvalidOperationException).FullName,
            parked.GetProperty("error").GetProperty("type").GetString());
        Assert.Equal("boom", parked.GetProperty("error").GetProperty("message").GetString());
        Assert.Equal(Deliveries.Queue, parked.GetProperty("queue").GetString());
        Assert.Equal(Deliveries.Exchange, parked.GetProperty("exchange").GetString());
        Assert.Equal(10ul, parked.GetProperty("deliveryTag").GetUInt64());
        Assert.Equal("pepe", parked.GetProperty("message").GetProperty("name").GetString());

        Assert.Equal(
            typeof(InvalidOperationException).FullName,
            Deliveries.Header(headers, TransportHeaders.ErrorType));
        Assert.Equal(Deliveries.Queue, Deliveries.Header(headers, TransportHeaders.ErrorGroupId));
    }

    [Fact]
    public async Task ParkedHeaders_CarryTheFailedMessageTrace()
    {
        Guid aggregateId = Guid.NewGuid();
        Guid aggregateCorrelationId = Guid.NewGuid();
        CommandErrorContext<TestCommand> context = new CommandErrorContext<TestCommand>(
            new TestCommand("pepe"),
            Deliveries.Transport(aggregateId: aggregateId, aggregateCorrelationId: aggregateCorrelationId),
            new InvalidOperationException());

        await CommandError().Handle(context, TestContext.Current.CancellationToken);

        IReadOnlyDictionary<string, string> headers = Assert.Single(_producer.Produced).Headers;
        Assert.Equal(aggregateId.ToString(), headers[TransportHeaders.AggregateId]);
        Assert.Equal(aggregateCorrelationId.ToString(), headers[TransportHeaders.AggregateCorrelationId]);
    }

    [Fact]
    public async Task ParkedBody_CarriesTheFullExceptionChain_AndTheHost()
    {
        Exception failure = new InvalidOperationException(
            "outer",
            new FormatException("the real cause"));
        failure.Data["field"] = "required";
        CommandErrorContext<TestCommand> context = new CommandErrorContext<TestCommand>(
            new TestCommand("pepe"),
            Deliveries.Transport(),
            failure);

        await CommandError().Handle(context, TestContext.Current.CancellationToken);

        JsonElement body = JsonSerializer.Deserialize<JsonElement>(Assert.Single(_producer.Produced).Body);
        JsonElement error = body.GetProperty("error");
        Assert.Equal("the real cause", error.GetProperty("innerError").GetProperty("message").GetString());
        Assert.Contains(nameof(FormatException), error.GetProperty("innerError").GetProperty("type").GetString());
        Assert.Equal("required", error.GetProperty("data").GetProperty("field").GetString());
        Assert.Equal(Environment.MachineName, body.GetProperty("machineName").GetString());
    }

    [Fact]
    public async Task ZeroInterval_RepublishesToTheExchange()
    {
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.Zero));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        (string exchange, string routingKey, _, IReadOnlyDictionary<string, string> headers) = Assert.Single(
            _producer.Produced);
        Assert.Equal(Deliveries.Exchange, exchange);
        Assert.Equal(string.Empty, routingKey);
        Assert.Equal("1", Deliveries.Header(headers, TransportHeaders.RetryCount));
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task PositiveInterval_ParksThroughScheduler()
    {
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.FromMinutes(5)));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Scheduled, sut.Result);
        Assert.Empty(_producer.Produced);
        (string exchange, string queue, _, IReadOnlyDictionary<string, string> headers, _) = Assert.Single(
            _scheduler.Scheduled);
        Assert.Equal(Deliveries.Exchange, exchange);
        Assert.Equal(Deliveries.Queue, queue);
        Assert.Equal("1", Deliveries.Header(headers, TransportHeaders.RetryCount));
    }

    [Fact]
    public async Task PositiveInterval_WithoutScheduler_ParksAsTerminal()
    {
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.FromMinutes(5)), scheduler: false);

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        (string exchange, string routingKey, _, _) = Assert.Single(_producer.Produced);
        Assert.Equal(string.Empty, exchange);
        Assert.Equal($"{Deliveries.Queue}.error", routingKey);
        Assert.Empty(_scheduler.Scheduled);
    }

    [Fact]
    public async Task SchedulerFails_LeavesUnhandled()
    {
        _scheduler.Failure = new InvalidOperationException("scheduler down");
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.FromMinutes(5)));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task LadderExhausted_Parks()
    {
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.Zero, TimeSpan.Zero));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(retryCount: 2),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.Queue}.error", Assert.Single(_producer.Produced).RoutingKey);
    }

    [Fact]
    public async Task ExcludedException_Parks_InheritanceAware()
    {
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.Zero), ImmutableList.Create(typeof(BaseFailure)));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new DerivedFailure()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Parked, sut.Result);
        Assert.Equal($"{Deliveries.Queue}.error", Assert.Single(_producer.Produced).RoutingKey);
    }

    [Fact]
    public async Task RequeueBrokerFailure_LeavesUnhandled()
    {
        _producer.Failure = new BrokerFailure();
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.Zero));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
    }

    [Fact]
    public async Task RequeueUnexpectedError_LeavesFaulted()
    {
        _producer.Failure = new InvalidOperationException("unexpected");
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.Zero));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Faulted, sut.Result);
    }

    [Fact]
    public async Task SecondRetry_ContinuesTheCumulativeCount()
    {
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero));

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(retryCount: 1),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Retried, sut.Result);
        Assert.Equal("2", Deliveries.Header(Assert.Single(_producer.Produced).Headers, TransportHeaders.RetryCount));
    }

    [Fact]
    public async Task ShutdownWhileParking_LeavesUnhandled_Silently()
    {
        // a deploy cancels the token mid-park. Unhandled is the only safe answer — the delivery stays
        // unacked, so whoever is still running gets it — and it must be SILENT: the generic catch logs
        // "Producer failed." at Error, and a routine shutdown must not put an error in the log.
        RecordingLogger<TestCommand> logger = new RecordingLogger<TestCommand>();
        _producer.Failure = new OperationCanceledException();
        ErrorHandler sut = CommandError(logger: logger);

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
        Assert.DoesNotContain(logger.Logged, entry => entry.Level >= LogLevel.Error);
    }

    [Fact]
    public async Task ShutdownWhileScheduling_LeavesUnhandled_Silently()
    {
        // same during a delayed retry: cancelling the schedule must not read as a scheduler outage.
        RecordingLogger<TestCommand> logger = new RecordingLogger<TestCommand>();
        _scheduler.Failure = new OperationCanceledException();
        ErrorHandler sut = CommandError(ImmutableList.Create(TimeSpan.FromMinutes(5)), logger: logger);

        await sut.Handle(
            new CommandErrorContext<TestCommand>(
                new TestCommand("pepe"),
                Deliveries.Transport(),
                new InvalidOperationException()),
            TestContext.Current.CancellationToken);

        Assert.Equal(ErrorResult.Unhandled, sut.Result);
        Assert.Empty(_producer.Produced);
        Assert.DoesNotContain(logger.Logged, entry => entry.Level >= LogLevel.Error);
    }
}
