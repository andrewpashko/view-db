import Foundation
import Security

struct KeychainStore {
    private let service = "com.viewdb.postgres.credentials"

    func save(endpointKey: String, username: String, password: String) throws {
        let payload = "\(username)\n\(password)"
        guard let data = payload.data(using: .utf8) else {
            throw AppError.connectionFailure("Unable to encode credentials.")
        }

        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: endpointKey,
        ]

        SecItemDelete(query as CFDictionary)

        var addQuery = query
        addQuery[kSecValueData as String] = data
        addQuery[kSecAttrAccessible as String] = kSecAttrAccessibleWhenUnlocked

        let status = SecItemAdd(addQuery as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw AppError.connectionFailure("Failed to store credentials in Keychain (\(status)).")
        }
    }

    func load(endpointKey: String) -> (username: String, password: String)? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: endpointKey,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]

        var item: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &item)
        guard status == errSecSuccess,
              let data = item as? Data,
              let payload = String(data: data, encoding: .utf8) else {
            return nil
        }

        let parts = payload.split(separator: "\n", maxSplits: 1, omittingEmptySubsequences: false)
        guard parts.count == 2 else { return nil }

        return (username: String(parts[0]), password: String(parts[1]))
    }
}
