import Foundation

enum RowPageDirection: String, Sendable {
    case initial
    case next
    case previous
}

enum RowPagingStrategy: String, Sendable {
    case offset
    case keysetID
    case keysetPrimaryKey
    case keysetCTID

    var usesCursor: Bool {
        switch self {
        case .offset:
            return false
        case .keysetID, .keysetPrimaryKey, .keysetCTID:
            return true
        }
    }
}

struct RowPageRequest: Sendable {
    let limit: Int
    let direction: RowPageDirection
    let offset: Int
    let cursor: String?

    init(limit: Int, direction: RowPageDirection, offset: Int = 0, cursor: String? = nil) {
        self.limit = limit
        self.direction = direction
        self.offset = max(0, offset)
        self.cursor = cursor
    }
}

struct RowPage: Sendable {
    let columns: [String]
    let rows: [[String]]
    let limit: Int
    let offset: Int
    let hasNext: Bool
    let strategy: RowPagingStrategy
    let orderedByColumn: String?
    let nextCursor: String?
    let previousCursor: String?

    static let empty = RowPage(
        columns: [],
        rows: [],
        limit: 0,
        offset: 0,
        hasNext: false,
        strategy: .offset,
        orderedByColumn: nil,
        nextCursor: nil,
        previousCursor: nil
    )
}

enum RowIdentityValueType: String, Hashable, Sendable {
    case numeric
    case textual
}

enum RowIdentity: Hashable, Sendable {
    case offset(Int)
    case columnValue(column: String, value: String, valueType: RowIdentityValueType)
    case ctid(String)
}

struct TableCellValue: Hashable, Sendable {
    let previewText: String
    let isTruncated: Bool
}

struct TableRowItem: Identifiable, Hashable {
    let id: Int
    let identity: RowIdentity
    let values: [TableCellValue]
}

struct RowPagePreview: Sendable {
    let columns: [String]
    let rows: [TableRowItem]
    let limit: Int
    let offset: Int
    let hasNext: Bool
    let strategy: RowPagingStrategy
    let orderedByColumn: String?
    let nextCursor: String?
    let previousCursor: String?

    static let empty = RowPagePreview(
        columns: [],
        rows: [],
        limit: 0,
        offset: 0,
        hasNext: false,
        strategy: .offset,
        orderedByColumn: nil,
        nextCursor: nil,
        previousCursor: nil
    )
}
