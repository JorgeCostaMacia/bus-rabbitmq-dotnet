# JorgeCostaMacia.Bus.RabbitMQ.HealthChecks

**Health check for `JorgeCostaMacia.Bus.RabbitMQ`** — reports whether the bus's shared broker connection is open, for ASP.NET Core health endpoints (liveness/readiness). No probe connection, no extra I/O: the check reads the state of the one long-lived connection the transport already owns. A **separate package** so the transport stays free of the HealthChecks dependency.

[![NuGet](https://img.shields.io/nuget/v/JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ.HealthChecks/)
[![Downloads](https://img.shields.io/nuget/dt/JorgeCostaMacia.Bus.RabbitMQ.HealthChecks.svg)](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ.HealthChecks/)
[![Build](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/actions/workflows/main.yml)
[![License](https://img.shields.io/github/license/JorgeCostaMacia/bus-rabbitmq-dotnet.svg)](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/blob/main/LICENSE.txt)

---

## Install

```bash
dotnet add package JorgeCostaMacia.Bus.RabbitMQ.HealthChecks
```

## Usage

One `AddRabbitMQBus` call plugs the check onto the standard health-check pipeline — after `AddBusContext`, which registers the shared connection the check reads. Tag it (typically `"ready"`) to pick which health endpoints run it.

```csharp
services.AddBusContext(configuration,
    producer => producer.AddCommand<PlaceOrder>("orders"));

services.AddHealthChecks().AddRabbitMQBus(tags: ["ready"]);
```

The check reports:

- **Healthy** — the connection is open, *or it has never been opened yet*: the bus opens it lazily on the first send/consume, so an untouched connection is not a failure.
- **The registration's failure status** (`Unhealthy` by default; pass `failureStatus` to soften it) — the connection has dropped; the client's automatic recovery keeps retrying, and the check flips back to healthy the moment it succeeds.

The check registers under the name `bus-rabbitmq` (override it with `name` when one service runs several buses).

## Requirements

The **.NET 10** SDK.

Brings [JorgeCostaMacia.Bus.RabbitMQ](https://www.nuget.org/packages/JorgeCostaMacia.Bus.RabbitMQ/) (the bus over RabbitMQ) and [Microsoft.Extensions.Diagnostics.HealthChecks](https://www.nuget.org/packages/Microsoft.Extensions.Diagnostics.HealthChecks/) transitively.

## About

`JorgeCostaMacia.Bus.RabbitMQ.HealthChecks` is part of **[bus-rabbitmq-dotnet](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet)** — the RabbitMQ transport of [JorgeCostaMacia.Bus](https://github.com/JorgeCostaMacia/bus-dotnet) over the official RabbitMQ.Client, with its Quartz-backed delayed retry and health checks; it releases on its own, so a patch here republishes neither the contracts nor the other transport.

- **Repository:** [github.com/JorgeCostaMacia/bus-rabbitmq-dotnet](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet)
- **Issues & requests:** [open an issue](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/issues)
- **Contributing:** [CONTRIBUTING.md](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/blob/main/CONTRIBUTING.md)
- **Security:** [report a vulnerability](https://github.com/JorgeCostaMacia/bus-rabbitmq-dotnet/security/advisories/new)

**Author:** Jorge Costa Maciá

- [LinkedIn](https://www.linkedin.com/in/jorge-costa-macia-842817164/)
- [GitHub](https://github.com/JorgeCostaMacia/)
- [Bitbucket](https://bitbucket.org/jorgecostamacia/)
