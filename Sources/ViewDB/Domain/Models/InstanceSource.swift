import Foundation

enum InstanceSource: String, Codable, Sendable, CaseIterable {
    case brew
    case postgresApp
    case docker

    var displayName: String {
        switch self {
        case .brew:
            "Homebrew"
        case .postgresApp:
            "Postgres.app"
        case .docker:
            "Docker"
        }
    }
}
