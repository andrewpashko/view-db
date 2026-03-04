import Foundation

struct BrewDiscoveryProvider: DiscoveryProvider {
    let source: InstanceSource = .brew
    private let commandRunner: any ShellCommandRunning

    init(commandRunner: any ShellCommandRunning) {
        self.commandRunner = commandRunner
    }

    func discover() async -> [DiscoveredInstance] {
        let result = await commandRunner.run("brew", arguments: ["services", "list"])
        guard result.succeeded else {
            return []
        }

        let services = Self.parseServices(result.stdout)
        guard !services.isEmpty else {
            return []
        }

        let socketCandidates = [
            "/tmp/.s.PGSQL.5432",
            "/opt/homebrew/var/run/postgresql/.s.PGSQL.5432",
            "/usr/local/var/run/postgresql/.s.PGSQL.5432",
        ]

        let existingSockets = socketCandidates.filter { FileManager.default.fileExists(atPath: $0) }
        let user = ProcessInfo.processInfo.environment["USER"]

        return services.map { service in
            DiscoveredInstance(
                source: .brew,
                displayName: "\(service.name) (\(service.status))",
                host: existingSockets.isEmpty ? "localhost" : nil,
                port: existingSockets.isEmpty ? 5432 : nil,
                socketPath: existingSockets.first,
                defaultUser: user,
                defaultDatabase: "postgres"
            )
        }
    }
}

extension BrewDiscoveryProvider {
    struct BrewService: Equatable {
        let name: String
        let status: String
    }

    static func parseServices(_ output: String) -> [BrewService] {
        output
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map(String.init)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter {
                !$0.isEmpty &&
                !$0.hasPrefix("Name") &&
                !$0.hasPrefix("✔") &&
                !$0.hasPrefix("Warning")
            }
            .compactMap { line in
                let parts = line.split(whereSeparator: { $0.isWhitespace })
                guard parts.count >= 2 else { return nil }
                let name = String(parts[0])
                guard name.hasPrefix("postgresql") else { return nil }
                return BrewService(name: name, status: String(parts[1]))
            }
    }
}
