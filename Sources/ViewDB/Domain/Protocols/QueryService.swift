import Foundation

protocol QueryService: Sendable {
    func fetchRows(database: DatabaseRef, table: TableRef, request: RowPageRequest) async throws -> RowPage
    func fetchRowsPreview(
        database: DatabaseRef,
        table: TableRef,
        request: RowPageRequest,
        previewLimitChars: Int
    ) async throws -> RowPagePreview
    func fetchCellValue(database: DatabaseRef, table: TableRef, rowIdentity: RowIdentity, columnName: String) async throws -> String
    func fetchRowCount(database: DatabaseRef, table: TableRef) async throws -> Int
    func runReadOnlySQL(database: DatabaseRef, sql: String, limit: Int) async throws -> RowPage
}
