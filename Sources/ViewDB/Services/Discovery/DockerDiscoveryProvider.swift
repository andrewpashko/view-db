import Foundation

struct DockerDiscoveryProvider: DiscoveryProvider {
    let source: InstanceSource = .docker
    private let commandRunner: any ShellCommandRunning

    init(commandRunner: any ShellCommandRunning) {
        self.commandRunner = commandRunner
    }

    func discover() async -> [DiscoveredInstance] {
        let result = await commandRunner.run(
            "docker",
            arguments: ["ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Ports}}"]
        )
        guard result.succeeded else {
            return []
        }

        return Self.parseContainers(result.stdout).compactMap { container in
            guard let hostPort = container.hostPort else {
                return nil
            }
            return DiscoveredInstance(
                source: .docker,
                displayName: "\(container.name) (\(container.image))",
                host: "127.0.0.1",
                port: hostPort,
                socketPath: nil,
                defaultUser: "postgres",
                defaultDatabase: "postgres",
                containerID: container.id
            )
        }
    }
}

extension DockerDiscoveryProvider {
    struct Container: Equatable {
        let id: String
        let name: String
        let image: String
        let hostPort: Int?
    }

    static func parseContainers(_ output: String) -> [Container] {
        output.trimmedLines.compactMap { line in
            let parts = line.components(separatedBy: "\t")
            guard parts.count >= 4 else {
                return nil
            }

            let id = parts[0].singleLineTrimmed
            let name = parts[1].singleLineTrimmed
            let image = parts[2].singleLineTrimmed
            let ports = parts[3].singleLineTrimmed

            guard image.lowercased().contains("postgres") else {
                return nil
            }

            return Container(id: id, name: name, image: image, hostPort: parseHostPort(from: ports))
        }
    }

    private static func parseHostPort(from ports: String) -> Int? {
        guard !ports.isEmpty else { return nil }

        let pattern = #"(?:^|\s|,)(?:\S+:)?(\d+)->5432/tcp"#
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return nil
        }

        let range = NSRange(ports.startIndex..<ports.endIndex, in: ports)
        guard let match = regex.firstMatch(in: ports, options: [], range: range),
              match.numberOfRanges > 1,
              let hostPortRange = Range(match.range(at: 1), in: ports) else {
            return nil
        }

        return Int(ports[hostPortRange])
    }
}
