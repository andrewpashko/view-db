import Foundation
import Logging
import PostgresNIO

actor PostgresSessionPool {
    private let credentialsStore: CredentialsStore
    private var nextConnectionID: Int = 1
    private let logger = Logger(label: "com.viewdb.postgres")

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
    }

    func withConnection<Value: Sendable>(
        instance: DiscoveredInstance,
        database: String,
        operation: @Sendable (PostgresConnection, Logger) async throws -> Value
    ) async throws -> Value {
        let credential = await credentialsStore.credential(for: instance.endpointKey)
        let fallbackUser = instance.defaultUser ?? ProcessInfo.processInfo.environment["USER"] ?? "postgres"
        let username = credential?.username ?? fallbackUser
        let password = credential?.password ?? instance.defaultPassword

        do {
            let configuration = makeConfiguration(
                instance: instance,
                username: username,
                password: password,
                database: database
            )
            let connectionID = allocateConnectionID()
            let connection = try await PostgresConnection.connect(
                configuration: configuration,
                id: connectionID,
                logger: logger
            )

            defer {
                Task {
                    try? await connection.closeGracefully()
                }
            }

            _ = try await connection.query("SET default_transaction_read_only = on", logger: logger)
            _ = try await connection.query("SET statement_timeout = '15s'", logger: logger)

            return try await operation(connection, logger)
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
