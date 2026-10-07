// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "ElectricCircuitsSwift",
  platforms: [.iOS(.v16), .macOS(.v13)],
  products: [
    .library(name: "ElectricCircuitsSwift", targets: ["ElectricCircuitsSwift"]),
    .library(name: "ElectricCircuitsCollections", targets: ["ElectricCircuitsCollections"]),
    .library(
      name: "ElectricCircuitsCollectionsTesting", targets: ["ElectricCircuitsCollectionsTesting"]),
    .executable(
      name: "ElectricCircuitsSwiftRealStack", targets: ["ElectricCircuitsSwiftRealStack"]),
    .executable(
      name: "ElectricCircuitsSwiftEngineDSOutage",
      targets: ["ElectricCircuitsSwiftEngineDSOutage"]),
  ],
  targets: [
    .target(name: "ElectricCircuitsSwift", path: "Sources/ElectricCircuitsSwift"),
    .target(
      name: "ElectricCircuitsCollections",
      dependencies: ["ElectricCircuitsSwift"],
      path: "Sources/ElectricCircuitsCollections"
    ),
    .target(
      name: "ElectricCircuitsCollectionsTesting",
      dependencies: ["ElectricCircuitsCollections"]
    ),
    .executableTarget(
      name: "ElectricCircuitsSwiftRealStack",
      dependencies: ["ElectricCircuitsSwift"],
      path: "Sources/ElectricCircuitsSwiftRealStack"
    ),
    .executableTarget(
      name: "ElectricCircuitsSwiftEngineDSOutage",
      dependencies: ["ElectricCircuitsSwift"],
      path: "Sources/ElectricCircuitsSwiftEngineDSOutage"
    ),
    .testTarget(
      name: "ElectricCircuitsSwiftTests", dependencies: ["ElectricCircuitsSwift"],
      path: "Tests/ElectricCircuitsSwiftTests"),
    .testTarget(
      name: "ElectricCircuitsCollectionsTests",
      dependencies: ["ElectricCircuitsCollections", "ElectricCircuitsCollectionsTesting"],
      path: "Tests/ElectricCircuitsCollectionsTests"),
  ]
)
