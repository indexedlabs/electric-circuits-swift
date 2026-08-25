# Telemetry and OTLP/HTTP

``TelemetryReporter`` is an opt-in, bounded best-effort exporter for OTLP/HTTP JSON traces and
metrics. Configure endpoints and authorization with ``TelemetryConfiguration`` and inject a custom
``TelemetrySink`` when the application needs its own networking policy.

## Boundaries

Telemetry is not part of request correctness. Export failures are contained: they update
``TelemetryHealth`` and do not fail a client request or local materialization. The reporter has finite
queue and batch settings, an export deadline, sampling, and explicit `flush` and `shutdown` methods.
Those limits bound the reporter's own queued work; they do not promise end-to-end delivery or replace
application observability requirements.

## Redaction

The public telemetry boundary intentionally avoids credentials and raw response content. Core errors
and attributes do not retain cookies, authorization headers, response headers, server error bodies,
or provider diagnostics. `traceparent` is propagated only as a validated W3C trace context, not as an
arbitrary attribute.

An OTLP authorization header is supplied only through ``TelemetryConfiguration`` to the configured
telemetry endpoint. It must not be reused as a server authentication credential. Use an
application-owned transport/sink when collectors require proxying, client certificates, or custom
credential refresh.
