import XCTest
@testable import ViewDB

final class DiscoveryCoordinatorTests: XCTestCase {
    func testDeduplicatePrefersFirstEndpoint() {
        let first = DiscoveredInstance(
            source: .brew,
            displayName: "brew",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )
        let duplicate = DiscoveredInstance(
            source: .postgresApp,
            displayName: "pgapp",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )
        let unique = DiscoveredInstance(
            source: .docker,
            displayName: "docker",
            host: "127.0.0.1",
            port: 5440,
            socketPath: nil
        )

        let deduped = DiscoveryCoordinator.deduplicate([first, duplicate, unique])

        XCTAssertEqual(deduped.count, 2)
        XCTAssertEqual(deduped[0].displayName, "brew")
        XCTAssertEqual(deduped[1].displayName, "docker")
    }
}
