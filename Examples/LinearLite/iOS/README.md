# LinearLite iOS host

This directory contains a minimal XcodeGen-generated iOS 16 host for the reviewed
`LinearLiteApp` Swift package.

## Run in Simulator

From this directory, regenerate the checked project from `project.yml` with the repository's
reproducibility check (from the repository root):

```sh
Scripts/generate-linear-lite-host-project.sh
```

To generate a local project directly (with `xcodegen` on `PATH`):

```sh
xcodegen generate
xcrun simctl list devices available
xcodebuild -project LinearLiteHost.xcodeproj -scheme LinearLiteHost \
  -sdk iphonesimulator -configuration Debug \
  -derivedDataPath .derivedData build
```

Open `LinearLiteHost.xcodeproj` in Xcode and select any available iOS Simulator. The host uses
`http://127.0.0.1:3000` by default (a safe loopback development endpoint). Configure another
endpoint with the `ELECTRIC_CIRCUITS_BASE_URL` process environment variable or UserDefaults launch
argument:

```sh
-ELECTRIC_CIRCUITS_BASE_URL https://engine.example
```

The app creates its `DatabaseQueue` under the app's Application Support directory. The app entry
injects `URLSessionTransport`; authentication headers/cookies remain transport/server-owned.
