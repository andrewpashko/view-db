import SwiftUI

struct DataGridView: View {
    let columns: [String]
    let rows: [TableRowItem]

    @State private var hoveredRowID: TableRowItem.ID?

    private let columnWidth: CGFloat = 170
    private let hoverRowBackground = Color.accentColor.opacity(0.08)
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
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(hoveredRowID == row.id ? hoverRowBackground : Color.clear)
            .contentShape(Rectangle())
            .onHover { isHovered in
                if isHovered {
                    if hoveredRowID != row.id {
                        hoveredRowID = row.id
                    }
                } else if hoveredRowID == row.id {
                    hoveredRowID = nil
                }
            }
    }

    private struct GridColumn: Identifiable {
        let id: Int
        let title: String
    }
}
