# ``ElectricCircuitsSwift``

@Metadata {
    @DisplayName("Electric Circuits Swift")
}

An Apple-platform Swift client for the Electric Circuits native `/v1` HTTP interface.

The package deliberately separates transport, local persistence, stream materialization, lifecycle,
and optional telemetry. It does not own application authorization, database schema design, or an
offline-write policy.

## Overview

Start with ``ElectricCircuitsClient`` and an application-owned ``HTTPTransport``. Create an explicit
shape or subset request, then use ``ShapeSubscriptionCoordinator`` when a durable live stream and
materializer lifecycle are needed. The `LinearLite` package is an example GRDB provider; GRDB is not a
dependency of this library.

The supported first profile is Swift tools 6.0 on iOS 16 or newer and macOS 13 or newer. See
<doc:Support> and <doc:Semantic-versioning> for the compatibility policy.

## Topics

### Requests and transport

- <doc:Transport>
- ``ElectricCircuitsClient``
- ``HTTPTransport``
- ``URLSessionTransport``
- ``ShapeRequest``
- ``SubsetQuery``
- ``AggregateRequest``

### Local materialization

- <doc:Materialization>
- ``ElectricCircuitsStore``
- ``ShapeMaterializer``
- ``ShapeStreamReader``

### Subscription lifecycle

- <doc:Lifecycle-and-errors>
- ``ShapeSubscriptionCoordinator``
- ``ShapeSubscriptionState``
- ``ClientError``
- ``StreamError``

### Observability

- <doc:Telemetry>
- ``TelemetryConfiguration``
- ``TelemetryReporter``

### Compatibility

- <doc:Support>
- <doc:Semantic-versioning>
