import Foundation

protocol QueryService: Sendable {
    func fetchRows(database: DatabaseRef, table: TableRef, request: RowPageRequest) async throws -> RowPage
    func fetchRowCount(database: DatabaseRef, table: TableRef) async throws -> Int
    func runReadOnlySQL(database: DatabaseRef, sql: String, limit: Int) async throws -> RowPage
}
