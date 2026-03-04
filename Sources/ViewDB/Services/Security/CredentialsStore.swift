import Foundation

actor CredentialsStore {
    private let keychain: KeychainStore
    private var inMemoryCredentials: [String: (username: String, password: String)] = [:]

    init(keychain: KeychainStore = KeychainStore()) {
        self.keychain = keychain
    }

    func credential(for endpointKey: String) -> (username: String, password: String)? {
        if let memory = inMemoryCredentials[endpointKey] {
            return memory
        }
        if let keychainValue = keychain.load(endpointKey: endpointKey) {
            inMemoryCredentials[endpointKey] = keychainValue
            return keychainValue
        }
        return nil
    }

    func save(endpointKey: String, username: String, password: String, persist: Bool) throws {
        inMemoryCredentials[endpointKey] = (username, password)
        if persist {
            try keychain.save(endpointKey: endpointKey, username: username, password: password)
        }
    }
}
