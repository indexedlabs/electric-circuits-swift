# Supported profile

`ElectricCircuitsSwift` first supports the following public library profile:

- Swift tools version 6.0.
- iOS 16 and newer.
- macOS 13 and newer.
- The Rust engine's versioned native `/v1` REST contract represented by the checked-in native-v1
  contract corpus.

The package does not promise support for older operating systems, pre-Swift-6 toolchains, a legacy
ElectricSync/tRPC control protocol, or a particular persistence provider. The optional LinearLite
example is a separate Swift package with GRDB and has its own dependency resolution.

The CI profile is an unsigned generic iOS Simulator host build plus strict SwiftPM root and LinearLite
tests. It verifies compilation and the executable package-quality contract; it is not a claim that a
signed application, App Store submission, backend deployment, or every device/OS pair has been
qualified.

See [SemVer policy](SEMVER.md) for release-impact rules.
