namespace JorgeCostaMacia.Bus.RabbitMQ.IntegrationTests.Support;

/// <summary>
/// The shared signal the two fanout subscribers and the test all hold: each subscriber completes its
/// own task with the payload it received, so the test can await both independently and prove the one
/// published event reached both queues bound to the event's fanout exchange — real coverage of the
/// event worker over a fanout topology.
/// </summary>
public sealed class FanoutProbe
{
    private readonly TaskCompletionSource<string> _first = new TaskCompletionSource<string>(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<string> _second = new TaskCompletionSource<string>(
        TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes with the payload the first subscriber received.</summary>
    public Task<string> First => _first.Task;

    /// <summary>Completes with the payload the second subscriber received.</summary>
    public Task<string> Second => _second.Task;

    /// <summary>Signals the first subscriber's delivery.</summary>
    /// <param name="payload">The payload it received.</param>
    public void SignalFirst(string payload) => _first.TrySetResult(payload);

    /// <summary>Signals the second subscriber's delivery.</summary>
    /// <param name="payload">The payload it received.</param>
    public void SignalSecond(string payload) => _second.TrySetResult(payload);
}
