import SwiftUI

struct DatabaseHeaderChipView: View {
    let title: String
    let subtitle: String
    var showsBackground: Bool = true

    var body: some View {
        let content = HStack(spacing: 10) {
            Image(systemName: "cylinder.split.1x2")
                .font(.headline)
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .font(.headline)
                    .lineLimit(1)
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)

        if showsBackground {
            content
                .viewDBGlassCard(cornerRadius: 14)
        } else {
            content
        }
    }
}
