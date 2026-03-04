import Foundation

struct RowPage: Sendable {
    let columns: [String]
    let rows: [[String]]
    let limit: Int
    let offset: Int
    let hasNext: Bool

    static let empty = RowPage(columns: [], rows: [], limit: 0, offset: 0, hasNext: false)
}

struct TableRowItem: Identifiable, Hashable {
    let id: Int
    let values: [String]
}
