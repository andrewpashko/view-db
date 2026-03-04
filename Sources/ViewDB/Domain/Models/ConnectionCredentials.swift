import Foundation

struct ConnectionCredentials: Sendable {
    let username: String
    let password: String
    let saveToKeychain: Bool
}

struct CredentialPromptState: Identifiable, Sendable {
    let id = UUID()
    let endpointKey: String
    let defaultUsername: String
    let reason: String
}
