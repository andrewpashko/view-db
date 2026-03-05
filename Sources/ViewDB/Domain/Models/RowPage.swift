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

struct TableRowItem: Identifiable, Hashable {
    let id: Int
    let values: [String]
}
