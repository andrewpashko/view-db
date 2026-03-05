import SwiftUI

struct DataGridView: View {
    let columns: [String]
    let rows: [TableRowItem]

    private let columnWidth: CGFloat = 170
    private var tableColumns: [GridColumn] {
        Array(columns.enumerated()).map { index, title in
            GridColumn(id: index, title: title)
        }
    }

    var body: some View {
        Table(rows) {
            TableColumnForEach(tableColumns) { column in
                TableColumn(column.title) { row in
                    cell(for: row, at: column.id)
                }
                .width(columnWidth)
            }
        }
        .textSelection(.enabled)
        .overlay {
            RoundedRectangle(cornerRadius: 12)
                .strokeBorder(Color.secondary.opacity(0.15), lineWidth: 1)
        }
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }

    private func cell(for row: TableRowItem, at index: Int) -> some View {
        Text(row.values[safe: index] ?? "")
            .font(.system(.callout, design: .monospaced))
            .lineLimit(1)
            .frame(maxWidth: .infinity, alignment: .leading)
            // Keep row text start aligned with native Table header title insets.
            .padding(.trailing, 8)
            .padding(.vertical, 4)
    }

    private struct GridColumn: Identifiable {
        let id: Int
        let title: String
    }
}
