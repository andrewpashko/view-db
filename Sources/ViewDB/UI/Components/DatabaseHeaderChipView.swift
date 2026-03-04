import SwiftUI

struct DatabaseHeaderChipView: View {
    let title: String
    let subtitle: String

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: "cylinder.split.1x2")
                .font(.headline)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.headline)
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .viewDBGlassCard(cornerRadius: 14)
    }
}
