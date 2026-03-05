import SwiftUI

struct SQLSheetView: View {
    @Binding var sqlText: String

    let rowPage: RowPage
    let rows: [TableRowItem]
    let isRunning: Bool
    let errorMessage: String?
    let onRun: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("Read-only SQL")
                    .font(.title3.bold())
                Spacer()
                Button("Run") {
                    onRun()
                }
                .viewDBGlassButton(prominent: true)
                .disabled(isRunning)
            }

            TextEditor(text: $sqlText)
                .font(.system(.body, design: .monospaced))
                .frame(minHeight: 130)
                .padding(8)
                .viewDBGlassCard(cornerRadius: 12)

            if let errorMessage {
                Text(errorMessage)
                    .foregroundStyle(.orange)
                    .font(.caption)
            }

            if isRunning {
                ProgressView("Running query...")
            }

            if rowPage.columns.isEmpty {
                Spacer()
                Text("No rows returned")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                DataGridView(columns: rowPage.columns, rows: rows)
                    .frame(minHeight: 220)
            }
        }
        .padding(16)
        .frame(minWidth: 780, minHeight: 560)
    }
}
