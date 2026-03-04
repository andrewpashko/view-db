import Foundation

struct TableRef: Identifiable, Hashable, Codable, Sendable {
    let id: UUID
    let databaseID: UUID
    let schema: String
    let name: String

    init(databaseID: UUID, schema: String, name: String) {
        self.databaseID = databaseID
        self.schema = schema
        self.name = name
        self.id = StableID.uuid(for: "\(databaseID.uuidString)|\(schema)|\(name)")
    }

    var fullName: String {
        "\(schema).\(name)"
    }
}
