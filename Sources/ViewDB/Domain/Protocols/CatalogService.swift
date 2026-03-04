import Foundation

protocol CatalogService: Sendable {
    func listDatabases(on instance: DiscoveredInstance, includeSystem: Bool) async throws -> [DatabaseRef]
    func listTables(on database: DatabaseRef) async throws -> [TableRef]
}
