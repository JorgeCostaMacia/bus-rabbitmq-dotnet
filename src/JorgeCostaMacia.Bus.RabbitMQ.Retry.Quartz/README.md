# JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz

**Quartz-backed delayed retry for `JorgeCostaMacia.Bus.RabbitMQ`** — a persistent, clustered `IRetryScheduler` that parks a failed delivery as a durable [Quartz](https://www.nuget.org/packages/Quartz/) job and re-produces it to its exchange when the delay elapses — retrying every five minutes while the produce keeps failing, then staying parked as a dead-letter. The bus's immediate retries (a `00:00` interval) re-publish through RabbitMQ; the **positive** intervals of the retry ladder go through this scheduler. Register the scheduler on the **sending** service; the **worker fleet** just runs Quartz against the shared store.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz/)
[![Build](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-rabbitmq-dotnet.svg)](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz
```

## Usage

This package is **agnostic to the Quartz store** — you configure Quartz (its store, clustering and serialization) as usual; it only plugs the retry `IRetryScheduler` and its job onto that scheduler. The store must be **persistent and shared** between the sending service and the worker fleet: scheduling commits to the store *before* the delivery is acked, so an in-memory store would drop the retry on a restart.

**Sending service** — parks delayed retries as Quartz jobs:

```csharp
services
    .AddBusContext(configuration, producer => producer.AddCommand<PlaceOrder>("orders"), consumer => consumer.AddCommandHandler<PlaceOrder, PlaceOrderHandler>("orders.handler"))
    .AddQuartz(q => q.UsePersistentStore(store =>
    {
        store.UsePostgres(/* connection string */);   // any provider
        store.ConfigureStore(o => o.StoreJobDataAsStrings = true);  // string-only job data
        store.UseClustering();
        store.UseSystemTextJsonSerializer();
    }))
    .AddRetryContext();                                // registers the Quartz-backed IRetryScheduler
```

**Worker fleet** — runs Quartz against the *same* store; fires the jobs and re-produces the message. It needs the bus producer registered so the job can produce; no extra retry registration — Quartz's DI job factory resolves the job on its own:

```csharp
services
    .AddBusContext(configuration, producer => producer.AddCommand<PlaceOrder>("orders"))
    .AddQuartz(q => q.UsePersistentStore(/* the same store */))
    .AddQuartzHostedService();
```

Each parked retry is a **durable** job grouped under its exchange, named `messageId:retryCount` (traceable; the same delivery parked twice just overwrites itself) and described with the failing consumer queue. Its single trigger fires the produce **exactly at the scheduled time**, then repeats it **every five minutes** while it keeps failing — four re-executions after the first fire, the same semantics as the bus's retries. The produce publishes back to the exchange with an **empty routing key**, exactly like the bus's own retries, with the parked envelope intact — retry count incremented and, for an event, the retry targeted to the failing queue only. A successful produce deletes the job; with the attempts exhausted Quartz completes the trigger and the durable job stays **parked as the dead-letter**: trigger-less, visible in the store, re-firable with `IScheduler.TriggerJob` (it deletes itself when a re-fire finally succeeds). The produce exception bubbles out of the job, so any Quartz job listeners (e.g. `JorgeCostaMacia.Quartz.Serilog`) observe every failed attempt.

## Requirements

The **.NET 10** SDK.

Brings [JorgeCostaMacia.Bus.RabbitMQ](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ/) (the bus over RabbitMQ) and [Quartz](https://www.nuget.org/packages/Quartz/) transitively.

## About

`JorgeCostaMacia.Bus.RabbitMQ.Retry.Quartz` is part of **[bus-rabbitmq-dotnet](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet)** — the RabbitMQ transport of [JorgeCostaMacia.Bus](https://github.com/JorgeCostaMacia/bus-dotnet) over the official RabbitMQ.Client, with its Quartz-backed delayed retry and health checks; it releases on its own, so a patch here republishes neither the contracts nor the other transport.

- **Repository:** [github.com/JorgeCostaMacia/bus-rabbitmq-dotnet](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
