import Foundation

enum RowPageDirection: String, Equatable, Sendable {
    case initial
    case next
    case previous
}

enum RowPagingStrategy: String, Equatable, Sendable {
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

enum SortDirection: String, Hashable, Sendable {
    case ascending
    case descending
}

struct TableSort: Hashable, Sendable {
    let column: String
    let direction: SortDirection
}

struct RowPageRequest: Sendable {
    let limit: Int
    let direction: RowPageDirection
    let offset: Int
    let cursor: String?
    let sort: TableSort?

    init(limit: Int, direction: RowPageDirection, offset: Int = 0, cursor: String? = nil, sort: TableSort? = nil) {
        self.limit = limit
        self.direction = direction
        self.offset = max(0, offset)
        self.cursor = cursor
        self.sort = sort
    }
}

struct RowPage: Sendable {
    let columns: [String]
    let columnTypeNames: [String]
    let rows: [[String]]
    let limit: Int
    let offset: Int
    let hasNext: Bool
    let strategy: RowPagingStrategy
    let sort: TableSort?
    let nextCursor: String?
    let previousCursor: String?

    init(
        columns: [String],
        columnTypeNames: [String] = [],
        rows: [[String]],
        limit: Int,
        offset: Int,
        hasNext: Bool,
        strategy: RowPagingStrategy,
        sort: TableSort?,
        nextCursor: String?,
        previousCursor: String?
    ) {
        self.columns = columns
        self.columnTypeNames = columnTypeNames
        self.rows = rows
        self.limit = limit
        self.offset = offset
        self.hasNext = hasNext
        self.strategy = strategy
        self.sort = sort
        self.nextCursor = nextCursor
        self.previousCursor = previousCursor
    }

    static let empty = RowPage(
        columns: [],
        columnTypeNames: [],
        rows: [],
        limit: 0,
        offset: 0,
        hasNext: false,
        strategy: .offset,
        sort: nil,
        nextCursor: nil,
        previousCursor: nil
    )
}

enum RowIdentityValueType: String, Hashable, Sendable {
    case numeric
    case textual
}

enum RowIdentity: Hashable, Sendable {
    case offset(Int, sort: TableSort? = nil)
    case columnValue(column: String, value: String, valueType: RowIdentityValueType)
    case ctid(String)
}

struct TableCellValue: Hashable, Sendable {
    let previewText: String
    let isTruncated: Bool
}

struct RowEditKey: Hashable, Sendable {
    let columnName: String
    let value: String
    let typeName: String
}

struct RowEditLocator: Hashable, Sendable {
    let keys: [RowEditKey]

    var isEmpty: Bool {
        keys.isEmpty
    }
}

enum ColumnEditorKind: Hashable, Sendable {
    case textField
    case textArea
    case boolean
    case enumeration(options: [String])
}

struct ColumnEditDescriptor: Hashable, Sendable {
    let columnName: String
    let typeName: String
    let isEditable: Bool
    let isNullable: Bool
    let hasDefaultValue: Bool
    let isGenerated: Bool
    let editorKind: ColumnEditorKind
}

struct TableRowItem: Identifiable, Hashable {
    let id: Int
    let identity: RowIdentity
    let values: [TableCellValue]
    let editLocator: RowEditLocator?

    init(id: Int, identity: RowIdentity, values: [TableCellValue], editLocator: RowEditLocator? = nil) {
        self.id = id
        self.identity = identity
        self.values = values
        self.editLocator = editLocator
    }
}

struct RowPagePreview: Sendable {
    let columns: [String]
    let columnTypeNames: [String]
    let rows: [TableRowItem]
    let limit: Int
    let offset: Int
    let hasNext: Bool
    let strategy: RowPagingStrategy
    let sort: TableSort?
    let nextCursor: String?
    let previousCursor: String?

    init(
        columns: [String],
        columnTypeNames: [String] = [],
        rows: [TableRowItem],
        limit: Int,
        offset: Int,
        hasNext: Bool,
        strategy: RowPagingStrategy,
        sort: TableSort?,
        nextCursor: String?,
        previousCursor: String?
    ) {
        self.columns = columns
        self.columnTypeNames = columnTypeNames
        self.rows = rows
        self.limit = limit
        self.offset = offset
        self.hasNext = hasNext
        self.strategy = strategy
        self.sort = sort
        self.nextCursor = nextCursor
        self.previousCursor = previousCursor
    }

    static let empty = RowPagePreview(
        columns: [],
        columnTypeNames: [],
        rows: [],
        limit: 0,
        offset: 0,
        hasNext: false,
        strategy: .offset,
        sort: nil,
        nextCursor: nil,
        previousCursor: nil
    )
}
