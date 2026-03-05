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
        XCTAssertEqual(containers[0].host, "0.0.0.0")
        XCTAssertEqual(containers[0].hostPort, 5440)
    }

    func testParseContainersIncludesPostgresCompatibleImageByPortMapping() {
        let output = """
        a1\tmetrics-db\ttimescale/timescaledb:latest\t0.0.0.0:5439->5432/tcp
        """

        let containers = DockerDiscoveryProvider.parseContainers(output)

        XCTAssertEqual(containers.count, 1)
        XCTAssertEqual(containers[0].image, "timescale/timescaledb:latest")
        XCTAssertEqual(containers[0].hostPort, 5439)
    }

    func testParseContainersParsesTripleColonPortFormat() {
        let output = """
        a1\tpg\tpostgres:16\t:::5441->5432/tcp
        """

        let containers = DockerDiscoveryProvider.parseContainers(output)

        XCTAssertEqual(containers.count, 1)
        XCTAssertEqual(containers[0].hostPort, 5441)
    }

    func testDiscoverFallsBackToCommonDockerBinaryPaths() async {
        let output = """
        abc123\tpg-dev\tpostgres:17\t192.168.1.22:5440->5432/tcp
        """
        let commandRunner = MockShellCommandRunner(
            responses: [
                "docker": .init(status: 127, stdout: "", stderr: "not found"),
                "/opt/homebrew/bin/docker ps --format {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Ports}}": .init(status: 0, stdout: output, stderr: ""),
                "/opt/homebrew/bin/docker inspect --format {{range .Config.Env}}{{println .}}{{end}} abc123": .init(
                    status: 0,
                    stdout: "POSTGRES_USER=ok_api\nPOSTGRES_PASSWORD=ok_api_password\nPOSTGRES_DB=opplevelseskortet\n",
                    stderr: ""
                )
            ]
        )
        let provider = DockerDiscoveryProvider(commandRunner: commandRunner)

        let instances = await provider.discover()
        let commands = await commandRunner.commands

        XCTAssertEqual(instances.count, 1)
        XCTAssertEqual(instances[0].displayName, "pg-dev (postgres:17)")
        XCTAssertEqual(instances[0].host, "192.168.1.22")
        XCTAssertEqual(instances[0].port, 5440)
        XCTAssertEqual(instances[0].defaultUser, "ok_api")
        XCTAssertEqual(instances[0].defaultPassword, "ok_api_password")
        XCTAssertEqual(instances[0].defaultDatabase, "opplevelseskortet")
        XCTAssertEqual(commands.prefix(2), [
            "docker ps --format {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Ports}}",
            "/opt/homebrew/bin/docker ps --format {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Ports}}"
        ])
    }
}

private actor MockShellCommandRunner: ShellCommandRunning {
    private let responses: [String: ShellCommandResult]
    private(set) var commands: [String] = []

    init(responses: [String: ShellCommandResult]) {
        self.responses = responses
    }

    func run(_ command: String, arguments: [String]) async -> ShellCommandResult {
        let key = ([command] + arguments).joined(separator: " ")
        commands.append(key)
        return responses[key] ?? ShellCommandResult(status: 127, stdout: "", stderr: "not found")
    }
}
