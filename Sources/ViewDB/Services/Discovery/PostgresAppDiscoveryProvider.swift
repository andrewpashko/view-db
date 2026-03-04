import Foundation

struct PostgresAppDiscoveryProvider: DiscoveryProvider {
    let source: InstanceSource = .postgresApp

    init() {}

    func discover() async -> [DiscoveredInstance] {
        guard FileManager.default.fileExists(atPath: "/Applications/Postgres.app") else {
            return []
        }

        let socketCandidates = [
            "/tmp/.s.PGSQL.5432",
            "/tmp/.s.PGSQL.5433",
            "/tmp/.s.PGSQL.5434",
        ]

        let user = ProcessInfo.processInfo.environment["USER"]
        let socketInstances = socketCandidates
            .filter { FileManager.default.fileExists(atPath: $0) }
            .map { socket in
                let portText = socket.split(separator: ".").last ?? "5432"
                let port = Int(portText) ?? 5432
                return DiscoveredInstance(
                    source: .postgresApp,
                    displayName: "Postgres.app (:\(port))",
                    host: nil,
                    port: nil,
                    socketPath: socket,
                    defaultUser: user,
                    defaultDatabase: "postgres"
                )
            }

        if !socketInstances.isEmpty {
            return socketInstances
        }

        return [
            DiscoveredInstance(
                source: .postgresApp,
                displayName: "Postgres.app (localhost:5432)",
                host: "localhost",
                port: 5432,
                socketPath: nil,
                defaultUser: user,
                defaultDatabase: "postgres"
            ),
        ]
    }
}
