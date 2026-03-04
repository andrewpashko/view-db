import Foundation
import PostgresNIO

enum PostgresErrorClassifier {
    static func credentialPromptState(for error: any Error, instance: DiscoveredInstance) -> CredentialPromptState? {
        guard isAuthenticationFailure(error) else {
            return nil
        }

        return CredentialPromptState(
            endpointKey: instance.endpointKey,
            defaultUsername: instance.defaultUser ?? ProcessInfo.processInfo.environment["USER"] ?? "postgres",
            reason: "Authentication failed for \(instance.displayName)."
        )
    }

    static func isAuthenticationFailure(_ error: any Error) -> Bool {
        if let appError = error as? AppError,
           case .credentialsRequired = appError {
            return true
        }

        if let pgError = error as? PSQLError {
            if pgError.code == .authMechanismRequiresPassword {
                return true
            }
            if let state = pgError.serverInfo?[.sqlState], ["28000", "28P01"].contains(state) {
                return true
            }
        }

        let text = String(reflecting: error).lowercased()
        return text.contains("password authentication failed") ||
            text.contains("no password supplied") ||
            text.contains("authentication")
    }

    static func message(for error: any Error) -> String {
        if let appError = error as? AppError,
           let description = appError.errorDescription {
            return description
        }

        if let pgError = error as? PSQLError {
            if let serverMessage = pgError.serverInfo?[.message] {
                return serverMessage
            }
            return "Database operation failed (\(pgError.code.description))."
        }

        return (error as? LocalizedError)?.errorDescription ?? String(describing: error)
    }
}
