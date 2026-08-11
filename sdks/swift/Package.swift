// swift-tools-version: 6.3
import PackageDescription

let package = Package(
    name: "SmolVMSDK",
    platforms: [.macOS("11.0")],
    products: [
        .library(name: "SmolVMSDK", targets: ["SmolVMSDK"]),
    ],
    targets: [
        .target(
            name: "SmolVMSDK",
            swiftSettings: [
                .swiftLanguageMode(.v6),
                .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
            ]
        ),
        .testTarget(
            name: "SmolVMSDKTests",
            dependencies: ["SmolVMSDK"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
