# Transport and native API

``ElectricCircuitsClient`` sends Codable request and response models to the Rust engine's native,
versioned `/v1` REST API. Its public request models are the package's representation of the native
wire contract; the client does not speak tRPC.

## Authentication and request ownership

The library takes an ``HTTPTransport``. ``URLSessionTransport`` is the standard Foundation
implementation and accepts static headers, a cookie header, and Foundation cookies. Applications may
instead provide a transport that performs credential refresh, attaches tenant headers, or applies
their own request policy.

The core library neither persists nor interprets credentials. It deliberately does not copy request
headers, cookies, server error bytes, or response headers into public errors or telemetry attributes.
Authentication and authorization remain the transport and server's responsibility.

## Response admission

``ResponseDecodingLimits`` checks response bytes before JSON decoding. The default Foundation
transport applies the selected limit while it receives a successful body: its library-owned `Data`
retains at most `limit` bytes. Seeing the next byte produces the capped `limit + 1` observation,
cancels the URLSession task, and returns a typed oversized-response error. This covers chunked and
unknown-length bodies; a known successful `Content-Length` over the limit is rejected before that
library-owned accumulation. Foundation, URLSession, and the socket stack may buffer independently,
and the library does not control pooled-connection or TCP teardown timing. Error-status bodies are
not accumulated by this package, so native status and `Retry-After` classification remain
available. Custom ``HTTPTransport`` implementations retain their existing `send(_:)` contract and
must enforce any receive bound they require themselves. Likewise, the stream batch limit constrains
the batch admitted to the materializer; providers should set their own database and transaction
limits.

## Native contract compatibility

The checked-in `Contracts/native-v1-contract-v1.json` corpus is a compatibility fixture for the
currently supported `/v1` request and response encodings. Additive, backwards-compatible fields can
be accepted by a compatible client. A server that changes the meaning, requiredness, or encoding of a
field used by a supported client requires an appropriately versioned contract and release; see
<doc:Semantic-versioning>.
