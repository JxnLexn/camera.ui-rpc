// swift-tools-version: 5.9
import PackageDescription

let package = Package(
  name: "camera-ui-rpc",
  platforms: [
    .macOS(.v13),
    .iOS(.v16),
    .tvOS(.v16),
  ],
  products: [
    .library(name: "CameraUIRPC", targets: ["CameraUIRPC"])
  ],
  dependencies: [
    .package(path: "externals/nats.swift")
  ],
  targets: [
    .target(
      name: "CameraUIRPC",
      dependencies: [
        .product(name: "Nats", package: "nats.swift")
      ]
    ),
    .executableTarget(name: "codec-test", dependencies: ["CameraUIRPC"]),
    .executableTarget(name: "smoke-test", dependencies: ["CameraUIRPC"]),
    .testTarget(name: "CameraUIRPCTests", dependencies: ["CameraUIRPC"]),
  ]
)
