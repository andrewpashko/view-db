import Foundation

struct DockerDiscoveryProvider: DiscoveryProvider {
    let source: InstanceSource = .docker
    private let commandRunner: any ShellCommandRunning
    private let dockerCommands = [
        "docker",
        "/opt/homebrew/bin/docker",
        "/usr/local/bin/docker",
        "/Applications/Docker.app/Contents/Resources/bin/docker"
    ]

    init(commandRunner: any ShellCommandRunning) {
        self.commandRunner = commandRunner
    }

    func discover() async -> [DiscoveredInstance] {
        let result = await runDocker(arguments: ["ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Ports}}"])
        guard result.succeeded else {
            return []
        }

        var instances: [DiscoveredInstance] = []
        for container in Self.parseContainers(result.stdout) {
            guard let hostPort = container.hostPort else {
                continue
            }
            let host = await resolveHost(for: container, hostPort: hostPort)
            let defaults = await inspectDefaults(containerID: container.id)
            instances.append(
                DiscoveredInstance(
                source: .docker,
                displayName: "\(container.name) (\(container.image))",
                host: host,
                port: hostPort,
                socketPath: nil,
                defaultUser: defaults.user ?? "postgres",
                defaultPassword: defaults.password,
                defaultDatabase: defaults.database ?? "postgres",
                containerID: container.id
            )
            )
        }

        return instances
    }

    private func runDocker(arguments: [String]) async -> ShellCommandResult {
        var lastResult = ShellCommandResult(status: -1, stdout: "", stderr: "")

        for command in dockerCommands {
            let result = await commandRunner.run(command, arguments: arguments)
            if result.succeeded {
                return result
            }
            lastResult = result
        }

        return lastResult
    }

    private func inspectDefaults(containerID: String) async -> (user: String?, password: String?, database: String?) {
        let result = await runDocker(arguments: ["inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}", containerID])
        guard result.succeeded else {
            return (nil, nil, nil)
        }

        var user: String?
        var password: String?
        var database: String?

        for line in result.stdout.trimmedLines {
            if line.hasPrefix("POSTGRES_USER=") {
                user = String(line.dropFirst("POSTGRES_USER=".count))
            } else if line.hasPrefix("POSTGRES_PASSWORD=") {
                password = String(line.dropFirst("POSTGRES_PASSWORD=".count))
            } else if line.hasPrefix("POSTGRES_DB=") {
                database = String(line.dropFirst("POSTGRES_DB=".count))
            }
        }

        return (user, password, database)
    }

    private func resolveHost(for container: Container, hostPort: Int) async -> String {
        let normalizedRaw = (container.host ?? "127.0.0.1").singleLineTrimmed
        let normalized = normalizedRaw
            .trimmingCharacters(in: CharacterSet(charactersIn: "[]"))
            .lowercased()

        switch normalized {
        case "127.0.0.1", "localhost", "::1":
            return "127.0.0.1"
        case "0.0.0.0", "::", "":
            let localSocketPath = "/tmp/.s.PGSQL.\(hostPort)"
            if FileManager.default.fileExists(atPath: localSocketPath),
               let localIPv4 = await resolvePrimaryLocalIPv4() {
                return localIPv4
            }
            return "127.0.0.1"
        default:
            return normalizedRaw.trimmingCharacters(in: CharacterSet(charactersIn: "[]"))
        }
    }

    private func resolvePrimaryLocalIPv4() async -> String? {
        let commands: [(String, [String])] = [
            ("ipconfig", ["getifaddr", "en0"]),
            ("ipconfig", ["getifaddr", "en1"]),
            ("ipconfig", ["getifaddr", "bridge100"])
        ]

        for (command, arguments) in commands {
            let result = await commandRunner.run(command, arguments: arguments)
            if result.succeeded {
                let value = result.stdout.singleLineTrimmed
                if !value.isEmpty {
                    return value
                }
            }
        }

        return nil
    }
}

extension DockerDiscoveryProvider {
    struct Container: Equatable {
        let id: String
        let name: String
        let image: String
        let host: String?
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
            let binding = parsePublishedBinding(from: ports)
            let host = binding?.host
            let hostPort = binding?.port

            // Some Postgres-compatible images (for example timescaledb/postgis)
            // do not include "postgres" in the image name.
            guard hostPort != nil || image.lowercased().contains("postgres") else {
                return nil
            }

            return Container(id: id, name: name, image: image, host: host, hostPort: hostPort)
        }
    }

    private static func parsePublishedBinding(from ports: String) -> (host: String?, port: Int)? {
        guard !ports.isEmpty else { return nil }

        for rawBinding in ports.split(separator: ",") {
            let binding = String(rawBinding).singleLineTrimmed
            guard binding.contains("->5432/tcp"),
                  let arrowRange = binding.range(of: "->5432/tcp") else {
                continue
            }
            let lhs = String(binding[..<arrowRange.lowerBound]).singleLineTrimmed
            let portDigits = lhs.reversed().prefix { $0.isNumber }
            guard !portDigits.isEmpty else {
                continue
            }

            guard let port = Int(String(portDigits.reversed())) else {
                continue
            }

            let hostSuffix = lhs.dropLast(portDigits.count)
            var host = String(hostSuffix).trimmingCharacters(in: CharacterSet(charactersIn: ":"))
            if host.isEmpty, lhs.hasPrefix(":::") {
                host = "::"
            }

            return (host: host.isEmpty ? nil : host, port: port)
        }

        return nil
    }
}
