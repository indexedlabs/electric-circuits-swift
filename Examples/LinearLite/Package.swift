// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "LinearLiteGRDB",
  platforms: [.iOS(.v16), .macOS(.v13)],
  products: [
    .library(name: "LinearLiteGRDB", targets: ["LinearLiteGRDB"]),
    .library(name: "LinearLiteApp", targets: ["LinearLiteApp"]),
    .executable(name: "LinearLiteRealTopTen", targets: ["LinearLiteRealTopTen"]),
    .executable(name: "LinearLitePG18Failover", targets: ["LinearLitePG18Failover"]),
    .executable(
      name: "LinearLiteRealFilteredWindows", targets: ["LinearLiteRealFilteredWindows"]),
  ],
  dependencies: [
    .package(name: "electric-circuits-swift", path: "../.."),
    .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.0.0"),
  ],
  targets: [
    .target(
      name: "LinearLiteGRDB",
      dependencies: [
        .product(name: "ElectricCircuitsSwift", package: "electric-circuits-swift"),
        .product(name: "GRDB", package: "GRDB.swift"),
      ]
    ),
    .target(
      name: "LinearLiteApp",
      dependencies: [
        "LinearLiteGRDB",
        .product(name: "ElectricCircuitsSwift", package: "electric-circuits-swift"),
      ]
    ),
    // Deliberately qualification-only: the app and core library targets stay free of this
    // executable's phase gates and process-environment wiring.
    .executableTarget(
      name: "LinearLiteRealTopTen",
      dependencies: [
        "LinearLiteApp",
        "LinearLiteGRDB",
        .product(name: "ElectricCircuitsSwift", package: "electric-circuits-swift"),
        .product(name: "GRDB", package: "GRDB.swift"),
      ]
    ),
    // Qualification-only physical-standby failover adapter. It deliberately lives outside both
    // the core client and the example app target so GRDB remains an application-owned provider.
    .executableTarget(
      name: "LinearLitePG18Failover",
      dependencies: [
        .product(name: "ElectricCircuitsSwift", package: "electric-circuits-swift"),
        .product(name: "GRDB", package: "GRDB.swift"),
      ]
    ),
    // Qualification-only filtered-window adapter. The reusable view implementation stays in the
    // app/provider targets while process gates and real-stack wiring remain isolated here.
    .executableTarget(
      name: "LinearLiteRealFilteredWindows",
      dependencies: [
        "LinearLiteApp",
        "LinearLiteGRDB",
        .product(name: "ElectricCircuitsSwift", package: "electric-circuits-swift"),
        .product(name: "GRDB", package: "GRDB.swift"),
      ]
    ),
    .testTarget(
      name: "LinearLiteGRDBTests",
      dependencies: ["LinearLiteGRDB"],
      resources: [.copy("Fixtures")]
    ),
    .testTarget(
      name: "LinearLiteAppTests",
      dependencies: ["LinearLiteApp"]
    ),
  ]
)
