# Cursors, subscription lifecycle, and errors

``ShapeSubscriptionCoordinator`` owns one named remote shape claim, its stream reader, renewal,
release, and retry state. It exposes state changes through ``ShapeSubscriptionState`` and always
obtains the resume cursor from its ``ShapeMaterializer`` rather than treating process memory as the
durable checkpoint.

## Cursors and restart

``StreamCursor`` is opaque to application code. Store the cursor transactionally with the rows it
acknowledges. On a restart, pass the same materializer back to the coordinator so the reader resumes
only after the committed local cursor. Replaying an already committed cursor is idempotent for the
in-memory reference materializer; production providers must implement their own corresponding
idempotence and transactional guarantees.

## Lifecycle outcomes

A successful `start` creates or renews a named claim, then starts a long-poll reader. `stop` cancels
local work and releases the claim. A terminal stream outcome may require reseeding rather than an
automatic retry; it is reported as ``ShapeSubscriptionReseedRequired``. A caller must choose how to
acquire a new snapshot and replace its local materialization.

The coordinator's retry policy handles only classified transient HTTP and transport failures. It is
not an offline-write queue, conflict resolver, or background-execution scheduler.

## Error handling

``ClientError`` retains a public HTTP status/classification without server response data. ``StreamError``
distinguishes terminal stream results, malformed data, cursor conflicts, and unavailable local
materializers. Treat an unclassified failure as an application decision point rather than parsing
error strings or relying on undocumented retry behavior.
