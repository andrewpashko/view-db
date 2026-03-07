import Foundation

protocol CellEditingService: Sendable {
    func fetchEditMetadata(database: DatabaseRef, table: TableRef) async throws -> [ColumnEditDescriptor]
    func fetchEditableCellValue(
        database: DatabaseRef,
        table: TableRef,
        rowLocator: RowEditLocator,
        columnName: String
    ) async throws -> String?
    func updateCell(
        database: DatabaseRef,
        table: TableRef,
        rowLocator: RowEditLocator,
        columnName: String,
        value: String?
    ) async throws -> String
}
