// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "ViewDB",
    platforms: [
        .macOS(.v26),
    ],
    products: [
        .executable(name: "ViewDB", targets: ["ViewDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/postgres-nio.git", exact: "1.32.0"),
    ],
    targets: [
        .executableTarget(
            name: "ViewDB",
            dependencies: [
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
        ),
        .testTarget(
            name: "ViewDBTests",
            dependencies: ["ViewDB"]
        ),
        .testTarget(
            name: "ViewDBUITests",
            dependencies: ["ViewDB"]
        ),
    ]
)
