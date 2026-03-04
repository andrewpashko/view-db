import Foundation

protocol CredentialService: Sendable {
    func saveCredentials(for instance: DiscoveredInstance, credentials: ConnectionCredentials) async throws
}
