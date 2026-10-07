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

## Collection revalidation

The collections coordinator accepts a reporter in `startStaleRevalidation(clock:telemetry:)`.
Each run records a `sync.revalidate` span with these scalar attributes:

| Attribute | Meaning |
| --- | --- |
| `electric.table` | The collection definition's ID |
| `sync.subscription_kind` | The definition's optional fixed kind label; omitted when absent |
| `sync.outcome` | `refreshed`, `held`, `failed`, or `unrebuildable` |
| `sync.rows_returned` | Rows returned by the committed re-run snapshot, or zero if none landed |
| `sync.claims_released` | The observed net claim decrease described below |
| `sync.seconds_since_mark` | Monotonic seconds from first observation of this mark to run start |
| `sync.duration_seconds` | Monotonic elapsed seconds for this run |

Seconds since the mark starts when this coordinator first lists the mark in this process, not
when the store wrote it; the persisted mark is a source position, not a timestamp. Claims released
is `max(0, claimCount read with the mark before the run - rows returned by the re-run snapshot)`
for `refreshed`: it is the net decrease in the materialization's claims, so a run that both drops
and gains rows reports only the net. For `unrebuildable` it is the claim count read with the mark,
and for `held` and `failed` it is zero. It is not an exact transactional count of removed keys.

## Redaction

The public telemetry boundary intentionally avoids credentials and raw response content. Core errors
and attributes do not retain cookies, authorization headers, response headers, server error bodies,
or provider diagnostics. `traceparent` is propagated only as a validated W3C trace context, not as an
arbitrary attribute.

An OTLP authorization header is supplied only through ``TelemetryConfiguration`` to the configured
telemetry endpoint. It must not be reused as a server authentication credential. Use an
application-owned transport/sink when collectors require proxying, client certificates, or custom
credential refresh.
