# Stores and materialization

The package has two independent local-data seams.

``ElectricCircuitsStore`` is a small asynchronous `Data` cache. It is suitable for application-owned
snapshots and metadata, and ``InMemoryElectricCircuitsStore`` is a deterministic implementation for
tests and previews.

``ShapeMaterializer`` is the live-stream boundary. A provider receives a ``ChangeBatch`` and the
cursor it is advancing to in one call:

```swift
try await materializer.apply(batch, expecting: currentCursor, advancingTo: nextCursor)
```

The provider must make its row changes and cursor advancement one atomic durable operation. The
``ShapeStreamReader`` advances its process-local cursor only after that call returns. A successful
network read alone is therefore not an acknowledgement that the local cache has applied the batch.

## Scopes and overlapping views

``MaterializationScope`` identifies the principal, template, and generation that own a materialized
view. A provider can use it to isolate rows, cursors, and optimistic overlays for overlapping
subscriptions. The package does not prescribe a relational table layout or an optimistic-write model:
the LinearLite example demonstrates one GRDB topology rather than making it a library contract.

## Availability

Providers may report ``MaterializerAvailabilityError`` before the coordinator creates a remote shape.
This enables application policies such as protected-data and storage readiness without producing a
remote claim that cannot be materialized locally. Other provider errors are surfaced as typed
subscription failures; callers decide whether and when to retry.
