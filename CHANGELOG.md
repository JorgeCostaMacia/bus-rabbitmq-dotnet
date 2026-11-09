# Changelog

What a consumer of these packages has to act on, newest first. Versions that only touched the docs, the build or the internals are not listed.

## 5.1.0

The log messages are PascalCase event names, each the old text without its spaces and its final dot (`Subscriber failed.` is `SubscriberFailed`, `Connection recovered.` is `ConnectionRecovered`): a query, a dashboard or an alert on the old text needs the new one. `.Retry.Quartz` is built on Quartz 4.7, which validates `triggers.fixed_delay` on start: run Quartz's `database/migrations/4.7/add_trigger_fixed_delay` on every store before the first 4.7 node starts its scheduler.

## 5.0.0

The three packages continue here from bus-net with the same ids, in a major because `Event` now implements the `IDomainEvent` of `JorgeCostaMacia.DDD.DomainEvent` 11.0.0 (ddd-dotnet). Bump them together with that package and `JorgeCostaMacia.Bus` 4.5.0 (bus-dotnet), and rename `JorgeCostaMacia.DomainEvent` to `JorgeCostaMacia.DDD.DomainEvent` in your `using`s. The Kafka transport left for bus-kafka-dotnet, which releases on its own.
