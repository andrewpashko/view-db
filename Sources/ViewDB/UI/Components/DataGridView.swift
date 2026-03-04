import SwiftUI

struct DataGridView: View {
    let columns: [String]
    let rows: [TableRowItem]

    private let columnWidth: CGFloat = 170
    private let headerTint = Color(nsColor: .windowBackgroundColor).opacity(0.76)
    private let headerCellTint = Color.white.opacity(0.18)
    private let rowBackground = Color(nsColor: .textBackgroundColor)

    var body: some View {
        ZStack {
            rowBackground

            ScrollView([.horizontal, .vertical]) {
                LazyVStack(alignment: .leading, spacing: 0, pinnedViews: [.sectionHeaders]) {
                    Section {
                        ForEach(rows) { row in
                            HStack(spacing: 0) {
                                ForEach(Array(columns.enumerated()), id: \.offset) { index, _ in
                                    Text(row.values[safe: index] ?? "")
                                        .font(.system(.body, design: .monospaced))
                                        .lineLimit(1)
                                        .textSelection(.enabled)
                                        .frame(width: columnWidth, alignment: .leading)
                                        .padding(.horizontal, 8)
                                        .padding(.vertical, 6)
                                        .background(Color.clear)
                                        .overlay(alignment: .trailing) {
                                            Rectangle()
                                                .fill(Color.secondary.opacity(0.12))
                                                .frame(width: 0.5)
                                        }
                                }
                            }
                            .background(
                                row.id.isMultiple(of: 2) ? rowBackground : rowBackground.opacity(0.97)
                            )
                        }
                    } header: {
                        headerRow
                            .background {
                                Rectangle()
                                    .fill(.ultraThinMaterial)
                                    .overlay(headerTint)
                            }
                            .zIndex(2)
                    }
                }
            }
        }
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .overlay {
            RoundedRectangle(cornerRadius: 12)
                .strokeBorder(Color.secondary.opacity(0.15), lineWidth: 1)
        }
    }

    private var headerRow: some View {
        HStack(spacing: 0) {
            ForEach(Array(columns.enumerated()), id: \.offset) { _, title in
                Text(title)
                    .font(.system(.caption, design: .monospaced).weight(.bold))
                    .lineLimit(1)
                    .frame(width: columnWidth, alignment: .leading)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 9)
                    .background(headerCellTint)
                    .overlay(alignment: .trailing) {
                        Rectangle()
                            .fill(Color.secondary.opacity(0.18))
                            .frame(width: 0.5)
                    }
            }
        }
        .overlay(alignment: .bottom) {
            Rectangle()
                .fill(Color.secondary.opacity(0.2))
                .frame(height: 1)
        }
        .compositingGroup()
    }
}
