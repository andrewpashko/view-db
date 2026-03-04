import Foundation

enum AppError: LocalizedError, Sendable {
    case noEndpoint
    case notSupported(String)
    case credentialsRequired(endpointKey: String, username: String)
    case readOnlyViolation
    case queryRejected(String)
    case connectionFailure(String)

    var errorDescription: String? {
        switch self {
        case .noEndpoint:
            "Unable to resolve a database endpoint."
        case .notSupported(let message):
            message
        case .credentialsRequired:
            "Credentials are required to connect."
        case .readOnlyViolation:
            "Only read-only SQL statements are allowed."
        case .queryRejected(let reason):
            reason
        case .connectionFailure(let message):
            message
        }
    }
}
