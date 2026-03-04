import SwiftUI

struct DatabaseCardView: View {
    let database: DatabaseRef
    let instance: DiscoveredInstance

    var body: some View {
        HStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 6) {
                Text(database.name)
                    .font(.headline)
                Text(instance.endpointLabel)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            Text(instance.source.displayName)
                .font(.caption.bold())
                .padding(.horizontal, 10)
                .padding(.vertical, 5)
                .background(Color.primary.opacity(0.08), in: Capsule())
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 10)
        .background(
            RoundedRectangle(cornerRadius: 16)
                .fill(Color(nsColor: .controlBackgroundColor).opacity(0.98))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 16)
                .stroke(Color.secondary.opacity(0.14), lineWidth: 1)
        )
    }
}
