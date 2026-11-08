# bus-rabbitmq-dotnet — working in this repo

The RabbitMQ transport of the `JorgeCostaMacia.Bus` contracts — the concrete command/event bus over the official RabbitMQ.Client (v7, async), its Quartz-backed delayed retry and its health check — shipped on NuGet under `JorgeCostaMacia.Bus.RabbitMQ.*`. The contracts live in [bus-dotnet](https://github.com/JorgeCostaMacia/bus-dotnet) and the other transport in [bus-kafka-dotnet](https://github.com/JorgeCostaMacia/bus-kafka-dotnet); each releases on its own, so a patch here republishes neither. Moved out of **bus-net** in 5.0.0, keeping the package ids.

## Layout

- `src/<Package>/` — one package per folder: `JorgeCostaMacia.Bus.RabbitMQ` (the transport), `.RabbitMQ.Retry.Quartz` and `.RabbitMQ.HealthChecks` (its satellites). `test/<Package>.Tests/` — its tests, plus `<Package>.IntegrationTests/` where a suite needs the real broker or store. `assets/` — icons + social preview.
- **Internal visibility**: each project grants `InternalsVisibleTo` to its matching `.Tests`; the transport additionally grants it to its own satellites (and their `.Tests`) — deliberate, since the satellites extend the transport's internal seam and version in lockstep with it, in this repo.
- **3-tier `Directory.Build.props`**: **root** (repo identity + the single lockstep `VersionPrefix`; TFM `net10.0`; ImplicitUsings, Nullable, AnalysisLevel, EnforceCodeStyleInBuild, TreatWarningsAsErrors, the NuGet audit; `IsPackable` false by default) → **`src/`** (`IsPackable` true, package-output: icon / readme / license, SourceLink, symbols, `GenerateDocumentationFile`, pack of LICENSE/COPYRIGHT/icon/README) → **`test/`** (test settings). Each `src` csproj declares **only** `Description` / `PackageTags` and its references; everything else is inherited.

## Architecture — a concrete transport over agnostic contracts (locked)

- **Concrete-first**: the command/event distinction is defined here, not in the contracts — `Bus.RabbitMQ` ships `Command` / `Event` abstract records (implementing the contracts' message interfaces; `Event` also `IDomainEvent`), `CommandHandler<T>` / `EventSubscriber<T>` bases (`: IHandler<T, …Context<T>>`) and `IBus : ISenderBus<Command>, IPublisherBus<Event>`. The other transport mirrors the same simple names in its own namespace, so moving a service between transports is swapping a `global using`.
- **No requester, no query bus** — request/response is not part of the bus.
- **Ordering is a non-concern by design**: each consumer decides how it resolves conflicts — typically by a version its data carries, applied only when newer. `ITracedMessage.AggregateOccurredAt` can serve as that order. `AggregateId` is domain/tracing metadata, **not** a partition key.
- **MassTransit** stays out of the bus.

## Dependencies

- **On the contracts and on DDD**: `JorgeCostaMacia.Bus` (bus-dotnet) and `JorgeCostaMacia.DDD.DomainEvent` (ddd-dotnet — `Event` implements `IDomainEvent`) are **published** packages, pinned in `Directory.Packages.props` like any third-party one. Never a `ProjectReference` across repos. A new contracts version reaches this transport only when its reference is bumped here and the transport releases; one it does not need, it never takes.
- **Intra-repo**, the satellites → the transport: **`ProjectReference`** (lockstep; pack emits the nuspec `<dependency>` at the shared version).
- **The RabbitMQ client**: `RabbitMQ.Client` — third-party `PackageReference`, versioned in `Directory.Packages.props`.
- **Quartz 4** (`Quartz*` 4.6.2) — this upgrade is what forced the single `net10.0` target: **Quartz 4 ships only `lib/net10.0`** (3.20 shipped net462/net472/netstandard2.0/net8/net9/net10), so multi-targeting and Quartz 4 are mutually exclusive. Its breaking changes as they land here: `IJob.Execute` takes a `CancellationToken` and returns `ValueTask`; `ScheduleJob` / `AddJob` replaced the `replace` bool with `ScheduleJobOptions` / `AddJobOptions` (`…Options.Replacing`); the matchers moved from `Quartz.Impl.Matchers` to `Quartz`; `StdSchedulerFactory` is gone, replaced by `QuartzSchedulerBuilder.Create().UseProperties(…).Build()`; `IPersistentStoreBuilder.UseProperties` became `ConfigureStore(options => options.StoreJobDataAsStrings = true)`; and `IJobExecutionContext` lost `Get` / `Put` (use `MergedJobDataMap`) and gained `RetryAttempt`.
- **The Quartz 4 schema is validated on start and is not the 3.x one** — it adds `QRTZ_PAUSED_JOB_GRPS` and new columns (`RETRY_POLICY`, `RETRY_ATTEMPT`, `PREFERRED_NODE`), so a 3.x schema fails `ValidateSchema` at startup. Quartz 4 embeds the DDL per provider, so the integration fixtures no longer carry a hand-written copy of `tables_postgres.sql`: the test store calls **`store.ProvisionSchema()`** and Quartz creates whatever is missing. It only ever *creates* — it is not an upgrade, so a **live** 3.x database still needs `database/migrations/4.0/schema_30_to_40_upgrade_postgres.sql` run before the fleet starts on Quartz 4. Quartz 4.6 validates three more nullable columns (`SCHEDULER_STATE.QUARTZ_VERSION`, `FIRED_TRIGGERS.INTERRUPT_REQUESTED_TIME`, `TRIGGERS.MISFIRE_THRESHOLD`, from `database/migrations/4.6`): a live store is migrated before any 4.6 node starts, a worker that only parks retries included, since parking writes `TRIGGERS`. A 4.5 node never names them, so the migration can run first and the nodes roll after. 4.6.1 and 4.6.2 change no API and no schema; since 4.6.1 a scheduler start no longer resets a declared trigger's fire count, a retry pending at shutdown or a pause an operator set.

## Targets & stack

- TFM: **`net10.0`**, single-target — Quartz 4 ships only `lib/net10.0`. **Do not reintroduce a per-TFM conditional.**
- Tests: **xUnit v4 (the `xunit.v3` package, 4.x) on Microsoft.Testing.Platform v2 (MTP)** — test projects are `OutputType=Exe`, `dotnet test` runs MTP because the root **`global.json`** opts in, and `xunit.runner.visualstudio` is referenced for IDE discovery, as `testing-net` sets up. Running them needs the **.NET 10 SDK**.
- Source is **UTF-8 without BOM** (`.editorconfig` `charset = utf-8`). camelCase locals, PascalCase types, I-prefixed interfaces. Copyright year stays **2023** (deliberate — don't bump).
- **Explicit types everywhere — spell the type out.** Never `var`, never target-typed `new()`, never collection expressions `[]`: write `new Foo(...)`, `new byte[] { ... }`, `new List<T> { ... }`, `Array.Empty<T>()`. The `.editorconfig` sets all three to explicit, but only `var` is analyzer-enforced — the analyzer never reports the implicit forms. Develop (`dotnet-build-nuget` from github-workflows) guards target-typed `new(` and implicit `new[]` with a **grep step** that fails the build, and collection expressions `[…]` with a second one (`code-style-net`'s pattern: attributes, indexers, array types and list patterns never match it). Do not introduce any of them.

## Dependencies — Central Package Management

Third-party **and** the cross-repo `JorgeCostaMacia.*` versions are centralized in **`Directory.Packages.props`** (`ManagePackageVersionsCentrally=true`): add/bump as `<PackageVersion>`, reference in csproj **without** a `Version`. Intra-repo `Bus.RabbitMQ.*` deps are `ProjectReference`, not managed by CPM.

**The strict gates are on**: `TreatWarningsAsErrors` and the NuGet vulnerability audit on every restore (direct and transitive, every severity), so any warning — an advisory included — fails the build. The fix is a version bump or a transitive override in CPM, never `NoWarn`. A third-party version lands once it is three days old, a security fix the day it ships (see `build-net`). The root `nuget.config` clears every inherited source and keeps nuget.org alone: this library consumes nothing else, and an inherited second source would raise `NU1507`. It is **deny-read** for agents — hand changes to it to the user.

## CI / publishing

The three workflows call the reusable ones of [github-workflows](https://github.com/JorgeCostaMacia/github-workflows) (`@v1`): `develop.yml` runs `dotnet-build-nuget` on develop and PRs (the explicit-types and collection-expression greps, build, the `dotnet format` check, test, pack — no publish); `main.yml` runs the same build on a push to `main` and then pushes the `nuget` artifact to nuget.org with Trusted Publishing (`NuGet/login`, OIDC) — that job stays here because the nuget.org policy names this repo's `main.yml`; `release.yml` creates the GitHub Release of each `v*` tag. **The (central) `VersionPrefix` is the publish gate**: `--skip-duplicate` leaves an existing version alone, so only a release that moved it publishes.

## Branching & releases — GitFlow

Use the **`gitflow` skill** for any branch/release work.

- Feature/bugfix → `feature/`|`bugfix/<name>-<ts>` from develop → finish `--no-ff` into develop.
- Release → `release/<version>` from develop → bump the **single** `VersionPrefix` in the **root** props → Release Finish (merge develop+main, annotated tag `v<version>`, atomic push).
- git's **default merge message** (`--no-ff --no-edit`, never `-m`). Branch prefixes only: `feature` / `bugfix` / `release` / `hotfix`.

## Git etiquette

- Commit under **your own identity** — don't hardcode anyone's name/email.
- Keep history clean — **no** `Co-Authored-By` / AI-assistant trailers in commits or messages.
- Merges use git's **default** message (see *Branching & releases* above).

## Relevant skills

Skills that apply to this repo — let them trigger, or invoke explicitly. The first group is from `jorgecostamacia-agent-skills` (each `-net` skill brings the generic one it realizes); the rest from `dotnet-agent-skills` (the `dotnet/skills` marketplace).

A skill listed here but missing on this machine is one command away: `claude plugin marketplace add JorgeCostaMacia/skills` (or `dotnet/skills`) once, then `claude plugin install <skill>@<marketplace>`.

- **`bus-net`** (with **`messaging`**) — how services use this transport: the `Command`/`Event` records and their two constructors, thin handlers and subscribers, `Send`/`Publish` with the incoming transport, the retry ladder with `ValidationException` excluded, retries parked in Quartz and run by a separate jobs worker, the route and handler naming, the producer/admin/consumer maps. A change here is a change in what that skill describes: keep the two in step.
- **`logging-net`** (with **`logging`**) — the logging style the transport's logger follows: fixed messages, every variable value through the log context, correlation in every scope.
- **`code-style-net`** — how the C# is written: explicit types, the 120-column width with declarations and constructions vertical, comments wrapped too, the gates.
- **`build-net`** — the three-tier `Directory.Build.props`, the single `VersionPrefix`, CPM, the three-day package age, the NuGet audit as a build gate, the library's packing metadata, and the `nuget.config` base.
- **`testing-net`** (with **`testing`**) — xUnit v3 on Microsoft.Testing.Platform, the test project layout, `TestContext.Current.CancellationToken`, Testcontainers fixtures (RabbitMQ, Postgres).
- **`ddd`** — domain events: the `IDomainEvent` marker crossing into the transport's `Event` is this repo's core concept.
- **`clean-architecture`** / **`solid`** — the bus as the messaging Infrastructure/Presentation seam (consumers are driving adapters), and design review of the public surface.
- **`gitflow`** — all branch/release work.
- **`dotnet`** — C# language server + general .NET development.
- **`dotnet-msbuild`** — `Directory.Build.props`, project-file quality, CPM.
- **`dotnet-nuget`** — dependency management.
- **`dotnet-test`** / **`dotnet-test-migration`** — tests; the xUnit v4 / MTP v2 setup.
- **`dotnet-upgrade`** — target-framework migrations.

Not relevant to this repo (skip): `bus-kafka-net` (the Kafka decisions are bus-kafka-dotnet's), `validation-net`, `ddd-net`, `efcore-net` (no FluentValidation, value objects or EF here — messages are primitive DTO contracts; boundary validation is the consuming app's job), `clean-architecture-net`, `dotnet-aspnetcore` (that's http-net), `dotnet-ai`, `dotnet-maui`, `dotnet-blazor`, `dotnet-data`, `dotnet-template-engine`, `dotnet11`, `dotnet-diag`, `dotnet-advanced`.

## Build & test

```
dotnet format bus-rabbitmq-dotnet.slnx                  # apply .editorconfig (using order, whitespace) — run before committing
dotnet build  bus-rabbitmq-dotnet.slnx -c Release
dotnet test   bus-rabbitmq-dotnet.slnx -c Release       # MTP v2 via global.json (needs the .NET 10 SDK + Docker); --logger is VSTest-only (MTP0001)
dotnet pack   bus-rabbitmq-dotnet.slnx -c Release        # packs all packable; tests are IsPackable=false
```

The integration suites (RabbitMQ and Postgres via Testcontainers) are the slow ones and need **Docker running locally**: without a daemon `dotnet test` fails there rather than skipping.

Run **`dotnet format` before committing** — it applies the `.editorconfig` (using ordering, whitespace, line endings), the CLI equivalent of Visual Studio's *Code Cleanup*, and it is the normalizer for a `.cs` a shell tool wrote with LF. Develop checks it after the build with `dotnet format --verify-no-changes --no-restore`: the solution is clean, so the check only ever fails on what the push itself brought. `.gitattributes` checks `*.cs` out as CRLF (`eol=crlf`), so the Linux runner sees what the `.editorconfig` requires.
