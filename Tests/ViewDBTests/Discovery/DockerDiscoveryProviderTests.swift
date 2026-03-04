import XCTest
@testable import ViewDB

final class DockerDiscoveryProviderTests: XCTestCase {
    func testParseContainersExtractsPostgresHostPort() {
        let output = """
        abc123\tpg-dev\tpostgres:17\t0.0.0.0:5440->5432/tcp, [::]:5440->5432/tcp
        def456\tredis\tredis:latest\t0.0.0.0:6379->6379/tcp
        """

        let containers = DockerDiscoveryProvider.parseContainers(output)

        XCTAssertEqual(containers.count, 1)
        XCTAssertEqual(containers[0].id, "abc123")
        XCTAssertEqual(containers[0].name, "pg-dev")
        XCTAssertEqual(containers[0].hostPort, 5440)
    }
}
