# Semantic versioning

The public SwiftPM product follows Semantic Versioning. A public contract includes a non-underscored
symbol, documented behavior, or serialized native-v1 field on which a supported client can rely.

## Major

A major release is required for removal or semantic change of public API, raising the minimum
toolchain or deployment target, incompatible requiredness/encoding of native-v1 fields, or provider
schema changes that cannot safely migrate existing local rows and cursors.

## Minor

A minor release is appropriate for additive compatible API, optional native-v1 fields, opt-in
telemetry capability, or an automatic provider migration that preserves materialized rows and
cursors. A server feature is not compatible merely because it is additive: a client must be able to
ignore it safely or document/version the required contract.

## Patch

A patch release contains compatible bug fixes, internal refactors, documentation, tests, or build
changes. A patch that corrects unsafe behavior must identify the correction in its release notes.

Before `1.0.0`, the same categories communicate intent but consumers should pin an explicit
compatible range and review each `0.x` release. The repository policy is in `Policies/SEMVER.md`.

## Source compatibility

The supported promise is source compatibility, not a stable precompiled binary module interface.
Consumers rebuild the affected SwiftPM package after an update. A scoped clean LinearLite build in CI
guards against stale objects linked against a previously public symbol; it does not change the
native-v1 wire contract.
