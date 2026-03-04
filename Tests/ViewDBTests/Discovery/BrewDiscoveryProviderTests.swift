import XCTest
@testable import ViewDB

final class BrewDiscoveryProviderTests: XCTestCase {
    func testParseServicesFiltersPostgresOnly() {
        let output = """
        Name          Status  User   File
        redis         started user   file
        postgresql@14 started user   file
        postgresql@15 none    user   file
        """

        let services = BrewDiscoveryProvider.parseServices(output)

        XCTAssertEqual(services.count, 2)
        XCTAssertEqual(services[0], .init(name: "postgresql@14", status: "started"))
        XCTAssertEqual(services[1], .init(name: "postgresql@15", status: "none"))
    }
}
