import Foundation

struct DatabaseRef: Identifiable, Hashable, Codable, Sendable {
    let id: UUID
    let instanceID: UUID
    let name: String

    init(instanceID: UUID, name: String) {
        self.instanceID = instanceID
        self.name = name
        self.id = StableID.uuid(for: "\(instanceID.uuidString)|\(name)")
    }
}
