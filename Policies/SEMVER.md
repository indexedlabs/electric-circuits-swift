# Semantic versioning policy

The library follows Semantic Versioning for its public SwiftPM library product. Public means a
non-underscored symbol, documented behavior, or serialized native-v1 field that a supported client
is entitled to rely on. Documentation examples do not add a guarantee beyond their linked public
contract.

## Major releases

Release a **major** version when removing or changing the meaning of a public Swift API; raising the
minimum Swift tools version or supported iOS/macOS deployment target; changing a source-compatible
default in a way that changes correctness, privacy, or lifecycle behavior; or making a previously
optional native-v1 request/response field required or incompatible.

Provider schema changes are major when a supported provider cannot open or safely migrate an existing
cache without an application-directed migration. This includes changing the semantics of a persisted
cursor, scope identity, primary key, or optimistic-overlay record.

## Minor releases

Release a **minor** version for additive, backwards-compatible public APIs, optional native-v1 fields,
new opt-in telemetry attributes, new provider capabilities, or a provider schema migration that is
automatic, reversible where the provider promises it, and preserves existing materialized data and
cursors.

An additive server feature is not automatically client-compatible: the client must either ignore it
safely or negotiate/document the required native-v1 contract version.

## Patch releases

Release a **patch** version for backwards-compatible bug fixes, internal refactors, documentation,
tests, build/CI changes, and security fixes that do not change a supported public contract. A patch
may tighten behavior only when the prior behavior was demonstrably unsafe or contradicted the
documented contract; the release notes must identify that correction.

## Pre-1.0 note

Until the first `1.0.0` release, versions still use these categories to communicate compatibility.
Consumers should pin an explicit compatible range and review release notes for any `0.x` update.

## Source and binary compatibility

The policy promises source compatibility within a supported release line. This package does not yet
declare library evolution or a stable binary module interface, so clients should rebuild from source
after updating it. A stale local SwiftPM build cache can retain objects linked against a superseded
public symbol even when a defaulted API addition is source-compatible. CI therefore performs a scoped
clean build of the LinearLite example; consumers seeing that linker symptom should clean and rebuild
their affected package rather than treat it as a wire-contract migration.
