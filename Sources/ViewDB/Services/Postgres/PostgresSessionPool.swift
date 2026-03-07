import Foundation
import Logging
import PostgresNIO

actor PostgresSessionPool {
    private struct ConnectionKey: Hashable {
        let endpointKey: String
        let database: String
        let username: String
        let readOnly: Bool
    }

    private struct ManagedConnection {
        let connection: PostgresConnection
        var lastUsedAt: Date
    }

    private let credentialsStore: CredentialsStore
    private var nextConnectionID: Int = 1
    private let logger = Logger(label: "com.viewdb.postgres")
    private var connections: [ConnectionKey: ManagedConnection] = [:]
    private var pendingConnections: [ConnectionKey: Task<PostgresConnection, Error>] = [:]
    private let idleConnectionTTL: TimeInterval = 90

    init(credentialsStore: CredentialsStore) {
        self.credentialsStore = credentialsStore
    }

    func saveCredentials(for instance: DiscoveredInstance, credentials: ConnectionCredentials) async throws {
        try await credentialsStore.save(
            endpointKey: instance.endpointKey,
            username: credentials.username,
            password: credentials.password,
            persist: credentials.saveToKeychain
        )
        await invalidateConnections(forEndpointKey: instance.endpointKey)
    }

    func withConnection<Value: Sendable>(
        instance: DiscoveredInstance,
        database: String,
        readOnly: Bool = true,
        operation: @Sendable (PostgresConnection, Logger) async throws -> Value
    ) async throws -> Value {
        let credential = await credentialsStore.credential(for: instance.endpointKey)
        let fallbackUser = instance.defaultUser ?? ProcessInfo.processInfo.environment["USER"] ?? "postgres"
        let username = credential?.username ?? fallbackUser
        let password = credential?.password ?? instance.defaultPassword
        let key = ConnectionKey(endpointKey: instance.endpointKey, database: database, username: username, readOnly: readOnly)

        do {
            await evictIdleConnections()

            let connection = try await resolveConnection(
                key: key,
                instance: instance,
                database: database,
                username: username,
                password: password,
                readOnly: readOnly
            )
            try Task.checkCancellation()

            do {
                let value = try await operation(connection, logger)
                markConnectionUsed(for: key)
                return value
            } catch {
                if shouldRetryWithFreshConnection(error: error, connection: connection) {
                    await closeAndRemoveConnection(for: key)
                    let freshConnection = try await resolveConnection(
                        key: key,
                        instance: instance,
                        database: database,
                        username: username,
                        password: password,
                        readOnly: readOnly
                    )

                    let value = try await operation(freshConnection, logger)
                    markConnectionUsed(for: key)
                    return value
                }

                if connection.isClosed {
                    connections.removeValue(forKey: key)
                }
                throw error
            }
        } catch {
            if PostgresErrorClassifier.isAuthenticationFailure(error) {
                throw AppError.credentialsRequired(endpointKey: instance.endpointKey, username: username)
            }
            throw error
        }
    }

    private func allocateConnectionID() -> Int {
        defer { nextConnectionID += 1 }
        return nextConnectionID
    }

    private func resolveConnection(
        key: ConnectionKey,
        instance: DiscoveredInstance,
        database: String,
        username: String,
        password: String?,
        readOnly: Bool
    ) async throws -> PostgresConnection {
        if let existing = connections[key] {
            if existing.connection.isClosed {
                connections.removeValue(forKey: key)
            } else {
                markConnectionUsed(for: key)
                return existing.connection
            }
        }

        if let pending = pendingConnections[key] {
            let result = await pending.result
            switch result {
            case .success(let connection):
                if connection.isClosed {
                    pendingConnections.removeValue(forKey: key)
                    return try await resolveConnection(
                        key: key,
                        instance: instance,
                        database: database,
                        username: username,
                        password: password,
                        readOnly: readOnly
                    )
                }
                markConnectionUsed(for: key)
                return connection
            case .failure(let error):
                pendingConnections.removeValue(forKey: key)
                throw error
            }
        }

        let configuration = makeConfiguration(
            instance: instance,
            username: username,
            password: password,
            database: database
        )
        let connectionID = allocateConnectionID()
        let task = Task.detached(priority: nil) { [logger] in
            let connection = try await PostgresConnection.connect(
                configuration: configuration,
                id: connectionID,
                logger: logger
            )
            do {
                let readOnlyQuery = readOnly
                    ? "SET default_transaction_read_only = on"
                    : "SET default_transaction_read_only = off"
                _ = try await connection.query(PostgresQuery(unsafeSQL: readOnlyQuery), logger: logger).collect()
                _ = try await connection.query("SET statement_timeout = '15s'", logger: logger).collect()
                return connection
            } catch {
                if !connection.isClosed {
                    try? await connection.closeGracefully()
                }
                throw error
            }
        }
        pendingConnections[key] = task

        let result = await task.result
        pendingConnections.removeValue(forKey: key)

        switch result {
        case .success(let connection):
            if let existing = connections[key],
               !existing.connection.isClosed {
                if existing.connection !== connection {
                    try? await connection.closeGracefully()
                }
                markConnectionUsed(for: key)
                return existing.connection
            }

            connections[key] = ManagedConnection(connection: connection, lastUsedAt: Date())
            return connection
        case .failure(let error):
            throw error
        }
    }

    private func markConnectionUsed(for key: ConnectionKey) {
        guard var managed = connections[key] else { return }
        managed.lastUsedAt = Date()
        connections[key] = managed
    }

    private func shouldRetryWithFreshConnection(error: any Error, connection: PostgresConnection) -> Bool {
        if connection.isClosed {
            return true
        }

        let description = String(describing: error).lowercased()
        return description.contains("connection closed") || description.contains("broken pipe")
    }

    private func invalidateConnections(forEndpointKey endpointKey: String) async {
        let keys = connections.keys.filter { $0.endpointKey == endpointKey }
        for key in keys {
            await closeAndRemoveConnection(for: key)
        }
    }

    private func evictIdleConnections() async {
        let now = Date()
        let keysToEvict = connections.compactMap { key, managed -> ConnectionKey? in
            if managed.connection.isClosed {
                return key
            }

            if now.timeIntervalSince(managed.lastUsedAt) > idleConnectionTTL {
                return key
            }

            return nil
        }

        for key in keysToEvict {
            await closeAndRemoveConnection(for: key)
        }
    }

    private func closeAndRemoveConnection(for key: ConnectionKey) async {
        guard let managed = connections.removeValue(forKey: key) else { return }
        if !managed.connection.isClosed {
            try? await managed.connection.closeGracefully()
        }
    }

    private func makeConfiguration(
        instance: DiscoveredInstance,
        username: String,
        password: String?,
        database: String
    ) -> PostgresConnection.Configuration {
        if let socketPath = instance.socketPath {
            return PostgresConnection.Configuration(
                unixSocketPath: socketPath,
                username: username,
                password: password,
                database: database
            )
        }

        return PostgresConnection.Configuration(
            host: instance.host ?? "127.0.0.1",
            port: instance.port ?? 5432,
            username: username,
            password: password,
            database: database,
            tls: .disable
        )
    }
}
