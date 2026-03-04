import Foundation

protocol QueryService: Sendable {
    func fetchRows(database: DatabaseRef, table: TableRef, limit: Int, offset: Int) async throws -> RowPage
    func runReadOnlySQL(database: DatabaseRef, sql: String, limit: Int) async throws -> RowPage
}
