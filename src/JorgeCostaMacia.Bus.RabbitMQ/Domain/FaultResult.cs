namespace JorgeCostaMacia.Bus.RabbitMQ.Domain;

/// <summary>
/// How a fault handler left a broken delivery — read by the consumer (the orchestrator) after it
/// invokes the handler, since the handler's <c>Handle</c> returns <see cref="System.Threading.Tasks.Task"/>
/// by contract and cannot report it. The consumer acks the delivery on <see cref="Parked"/> and
/// leaves it unacked on <see cref="Unhandled"/>.
/// </summary>
internal enum FaultResult
{
    /// <summary>The handler could not park the delivery (a failed publish) — it stays unacked and redelivers.</summary>
    Unhandled,

    /// <summary>Parked to the fault queue.</summary>
    Parked
}
