# Support

The first supported profile is Swift tools version 6.0 on iOS 16 or newer and macOS 13 or newer.
The supported server surface is the Rust engine's native versioned `/v1` REST contract represented by
the checked-in native-v1 corpus.

This library does not support the legacy ElectricSync/tRPC control protocol, older operating systems,
pre-Swift-6 toolchains, or a specific persistence provider. The LinearLite GRDB provider is an
example package, not a core dependency or an alternative compatibility guarantee.

The package-quality CI gate performs strict SwiftPM and LinearLite tests, documentation conversion,
project reproduction, and an unsigned generic iOS host build. That compile-time evidence does not
replace signed-device, backend deployment, or App Store qualification.

For the release-impact rules behind this profile, see <doc:Semantic-versioning>. The repository's
release checklist is also recorded in `Policies/SUPPORT.md`.
