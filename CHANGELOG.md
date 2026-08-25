# Changelog

All notable changes to `ElectricCircuitsSwift` are documented here. Release impact follows the
[Semantic Versioning policy](Policies/SEMVER.md).

## 0.1.0 — Initial release

- Foundation-only SwiftPM client for Electric Circuits native `/v1` lifecycle, subset-query, and
  durable-stream APIs.
- `URLSessionTransport`, bounded response decoding, typed errors, and caller-owned authentication
  and retry policy boundaries.
- Deterministic in-memory store/materializer seams; the optional LinearLite example keeps its GRDB
  provider and iOS SwiftUI host outside the core package.
- Swift tools 6.0; iOS 16+ and macOS 13+ support profile.

This release is source-compatible within its supported line but does not promise a stable binary
module interface. Rebuild consumers from source after updating.
