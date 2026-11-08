# JorgeCostaMacia.Bus.RabbitMQ

**RabbitMQ transport for `JorgeCostaMacia.Bus`** — the concrete command/event bus over [RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client/): the `Command`/`Event` base records, the `Transport`, the `CommandContext<T>`/`EventContext<T>`, the ergonomic `CommandHandler<T>`/`EventSubscriber<T>` bases, and the producer/consumer wiring. A Wolverine-style topology — one exchange per message type (a command's `direct`, an event's `fanout`), each handler's queue bound straight to it. **Reference this one package** to run the bus on RabbitMQ — it brings the `JorgeCostaMacia.Bus.*` contracts transitively.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.RabbitMQ.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.RabbitMQ.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ/)
[![Build](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-rabbitmq-dotnet.svg)](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.RabbitMQ
```

## Usage

One `AddBusContext` call registers the whole bus over the `Bus:Connection` configuration section. The **producer** lambda maps each message this service sends or publishes to its exchange — a command's `direct`, an event's `fanout`; the optional **consumer** lambda registers this service's handlers, each on its own queue (name it after the consuming group, e.g. `orders.handler`, `billing.on.orders.placed.subscriber`) — omit it for a send-only service. Events mirror commands with the `AddEvent<TEvent>` / `AddEventSubscriber<TEvent, TEventSubscriber>` pair: the fanout exchange copies each event to every bound queue, so one queue per subscribing group.

```csharp
services.AddBusContext(configuration,
    producer => producer.AddCommand<PlaceOrder>("orders"),
    consumer => consumer.AddCommandHandler<PlaceOrder, PlaceOrderHandler>("orders.handler", retryIntervals: [TimeSpan.Zero, TimeSpan.Zero]));
```

A message is a record over the `Command` / `Event` base (the serializer's constructor on top, the convenient one below); a handler closes `CommandHandler<T>` / `EventSubscriber<T>` and is scoped — one instance per delivery. Delivery is **at-least-once and unordered**, so handlers must be idempotent — deduplicate by the message id or reconcile by its timestamp. Send and publish through the scoped `IBus`.

```csharp
public sealed record PlaceOrder : Command
{
    public string OrderId { get; init; }

    [JsonConstructor]
    public PlaceOrder(Guid aggregateId, Guid aggregateCorrelationId, DateTime aggregateOccurredAt, ImmutableList<string> aggregateConsumers, string orderId)
        : base(aggregateId, aggregateCorrelationId, aggregateOccurredAt, aggregateConsumers)
        => OrderId = orderId;

    public PlaceOrder(string orderId)
        : base(aggregateId: null, aggregateCorrelationId: null, aggregateOccurredAt: null, aggregateConsumers: null)
        => OrderId = orderId;
}

public sealed class PlaceOrderHandler : CommandHandler<PlaceOrder>
{
    public override Task Handle(CommandContext<PlaceOrder> context, CancellationToken cancellationToken = default)
        => Task.CompletedTask;   // context.Message, context.Transport, and the envelope facets (ids, conversation, retry count, host)
}
```

```csharp
public sealed class OrdersService(IBus bus)
{
    public Task Place(string orderId, CancellationToken cancellationToken = default)
        => bus.Send(new PlaceOrder(orderId), cancellationToken);
}
```

### Configuration

The `Bus:Connection` section configures the single RabbitMQ connection both sides share. `HostName`, `UserName` and `Password` are required (startup fails fast without them); the rest default to: `Ssl` `true` (secure by default — TLS with the host name as the certificate match), `Port` `5671` (the plain 5672 is never a fallback — going plain requires `Ssl` `false` AND an explicit port), `VirtualHost` `/`, `ClientProvidedName` the machine name, `AutomaticRecoveryEnabled` `true`.

```json
{
  "Bus": {
    "Connection": {
      "HostName": "rabbit",
      "UserName": "user",
      "Password": "pass",
      "Port": 5671,
      "VirtualHost": "/",
      "ClientProvidedName": "orders-api",
      "AutomaticRecoveryEnabled": true
    }
  }
}
```

### Retries and park queues

`retryIntervals` is the retry ladder — one delay per attempt. A `00:00` entry re-publishes the delivery to its exchange **immediately**, envelope cloned and `RetryCount` incremented; a **positive** delay parks the delivery through the optional `IRetryScheduler` to be re-published at its time — register [JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz/) to enable it; with no scheduler registered it parks as terminal, since it cannot be delayed. `retryExcludeExceptionTypes` skips the ladder for the listed exception types (inheritance-aware). The default is an empty ladder: the first failure parks.

Each queue gets two durable park queues, unbound — reached by name via the default exchange, and **born lazily on the first park**: an existing `.error`/`.fault` queue always means there was a failure, and once drained it can simply be deleted — the broker stays clean until the next incident. Parking is loss-proof regardless: each park (re)declares its queue first and publishes `mandatory`, so an unroutable park throws — nacking the original for redelivery — instead of vanishing silently. A terminal handling failure parks to **`{queue}.error`** as a typed error record (the original message fully typed, the whole failure chain and the transport details, with the original envelope cloned in the parked headers); a malformed delivery — undeserializable body, unreadable envelope — and a broken error lane park the raw delivery to **`{queue}.fault`**.

Treat a park queue as **exactly as sensitive as the traffic it shadows**: a `.error` record carries the original message body in full — the same domain data as the primary queue — plus the failure detail (exception type, message, stack trace, the inner-exception chain, and any `Exception.Data`), and a `.fault` record carries the raw delivery verbatim. So `{queue}.error` and `{queue}.fault` hold the same payloads as their source, sensitive ones included. Give them the **same vhost and user permissions** as the queue they shadow; never let the park queues pool into a broadly-readable "everything" lane that widens who can read those payloads. And since `Exception.Data` is copied into the parked record, handler authors should never attach a secret (token, credential, patient identifier) to it.

### Topology at startup

The producer declares its own topology: a hosted worker creates every mapped exchange (a command's `direct`, an event's `fanout`) before the app starts serving, so a send-only service does not depend on a consumer having created it. Each consumer declares its exchange and its durable queue bound to it; the `.error` / `.fault` park queues are not part of the startup topology — they are created by the first park. Every declare is idempotent — whoever arrives first creates it, the other's declare is a no-op.

## Guarantees & the consumer contract

Delivery is **at-least-once and unordered** by design, not by accident of tuning — the four points below are the contract every consuming service must hold to. They are not knobs to relax: reading them before writing a handler is the difference between a correct consumer and a subtly broken one.

- **At-least-once — handlers must be idempotent.** The same message can arrive more than once: a handling failure nacks the delivery for the broker to redeliver, and a delivery resolved but left unacked when its channel drops is redelivered on recovery. A handler must therefore tolerate reprocessing the same message with **no duplicated side effect** — deduplicate by the message id, or make the write naturally idempotent (upsert on a domain key, guard on the current state) so a second delivery is a no-op.
- **No guaranteed ordering.** A single queue drained by one consumer keeps order, but that is not a guarantee to lean on: a nacked delivery is requeued and comes back out of position, a prefetch above one hands several deliveries over at once, and a scaled-out group with more than one consumer interleaves them. Never assume the order two messages arrive in — reconcile against a version the data carries, applied only when newer, so a stale or reprocessed message can never overwrite a newer state. `AggregateOccurredAt`, the instant the event occurred, can serve as that version; a consumer whose data carries one of its own can compare that instead. A causal chain stays ordered on its own — a follow-up message is only produced once its cause has been handled — so this bites only messages that are genuinely concurrent.
- **Messages must be small — use the claim-check pattern.** Messages carry structured domain data, not payloads — a text invoice PDF is ~5–80 KB in Base64, comfortably small, but a scan, a high-resolution image or any file is not. Store large blobs externally (blob storage, the database) and send a small reference — the *claim-check* — in the message; the consumer fetches the blob only if it needs it. The bus is a data channel, not a file transfer.
- **Batch `Send`/`Publish` is not atomic.** A batch is published message by message over the channel; if one fails the others may already be on their exchanges, and because the delivery that triggered the batch is then retried, the **whole batch is re-published** on redelivery — so some of its messages are sent more than once. This is the at-least-once model showing through, not a defect: it is exactly why the first point holds — consumers must be idempotent.

## Wire format

Message bodies are JSON serialized with .NET's Web defaults — **camelCase** property names, case-insensitive reads. The envelope travels in **`jcm-`-prefixed headers** (`jcm-message-id`, `jcm-retry-count`, …), hyphenated like the header conventions of HTTP and AMQP (the broker's own `x-*` headers included); dictionary keys in bodies are user data and travel untouched.

Envelope headers travel as a **`string → string`** table of canonical text: the ids in their dashed GUID form (`jcm-message-id`, `jcm-conversation-id`, `jcm-aggregate-id`, `jcm-aggregate-correlation-id`), the counter (`jcm-retry-count`) as invariant digits, the dates as ISO round-trip. The RabbitMQ .NET client encodes each string value as an AMQP longstr, so the whole envelope is **human-readable in the management UI** — no base64 GUID blobs. The typed materialization (GUID, `int`, date) happens at the reading boundary on the incoming delivery, whose field table hands its values back as bytes. On Kafka the headers stay **raw bytes**, since its header API is natively binary — each transport is idiomatic to its broker.

## Logging

Log messages are fixed, low-cardinality grouping keys; every variable detail — exchange, queue, body, the decoded envelope headers, the `BusDescription` outcome expansion — travels as structured properties through Serilog's `LogContext`. Wire `.Enrich.FromLogContext()` into the host's Serilog pipeline (the usual default) so those properties reach the sinks.

## Requirements

The **.NET 10** SDK.

Brings [JorgeCostaMacia.Bus](https://www.nuget.org/packages/JorgeCostaMacia.Bus/) (the transport-agnostic contracts) transitively, plus [RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client/).

## About

`JorgeCostaMacia.Bus.RabbitMQ` is part of **[bus-rabbitmq-dotnet](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet)** — the RabbitMQ transport of [JorgeCostaMacia.Bus](https://github.com/JorgeCostaMacia/bus-dotnet) over the official RabbitMQ.Client, with its Quartz-backed delayed retry and health checks; it releases on its own, so a patch here republishes neither the contracts nor the other transport.

- **Repository:** [github.com/JorgeCostaMacia/bus-rabbitmq-dotnet](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
