import SwiftUI

struct DatabaseCardView: View {
    let database: DatabaseRef
    let instance: DiscoveredInstance
    var isHidden: Bool = false
    let onOpen: () -> Void
    let onToggleHidden: () -> Void

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Button {
                onOpen()
            } label: {
                cardContent
            }
            .buttonStyle(.plain)
            .accessibilityLabel("Open \(database.name)")
            .accessibilityHint("Open database tables")

            visibilityButton
                .padding(10)
        }
    }

    private var cardContent: some View {
        VStack(alignment: .leading, spacing: 12) {
            headerRow

            endpointRow

            statusRow

            Spacer(minLength: 0)
        }
        .padding(14)
        .frame(maxWidth: .infinity, minHeight: 148, alignment: .topLeading)
        .contentShape(RoundedRectangle(cornerRadius: 18))
        .viewDBGlassCard(interactive: true, cornerRadius: 18)
        .overlay(
            RoundedRectangle(cornerRadius: 18)
                .strokeBorder(Color.primary.opacity(0.08), lineWidth: 1)
        )
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(database.name), \(instance.source.displayName), \(instance.endpointLabel)")
        .opacity(isHidden ? 0.72 : 1.0)
    }

    private var visibilityButton: some View {
        Button {
            onToggleHidden()
        } label: {
            Image(systemName: isHidden ? "eye" : "eye.slash")
                .font(.caption.weight(.bold))
                .frame(width: 20, height: 20)
                .contentShape(Circle())
                .padding(2)
                .viewDBGlassCard(interactive: true, cornerRadius: 8)
        }
        .buttonStyle(.plain)
        .help(isHidden ? "Show database" : "Hide database")
        .accessibilityLabel(isHidden ? "Show database" : "Hide database")
    }

    private var headerRow: some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: "cylinder.split.1x2.fill")
                .font(.title3.weight(.semibold))
                .foregroundStyle(Color.accentColor)

            VStack(alignment: .leading, spacing: 4) {
                Text(database.name)
                    .font(.headline.weight(.semibold))
                    .lineLimit(2)
                    .truncationMode(.tail)

                Text(instance.displayName)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }

            Spacer(minLength: 4)
        }
        // Reserve space so long names never overlap the in-card visibility icon.
        .padding(.trailing, 26)
    }

    private var endpointRow: some View {
        HStack(spacing: 6) {
            Image(systemName: "network")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            Text(instance.endpointLabel)
                .font(.caption.monospaced())
                .foregroundStyle(.secondary)
                .lineLimit(1)
                .truncationMode(.middle)
        }
    }

    @ViewBuilder
    private var statusRow: some View {
        HStack(spacing: 8) {
            Text(instance.source.displayName)
                .font(.caption.bold())
                .padding(.horizontal, 10)
                .padding(.vertical, 5)
                .background(Color.primary.opacity(0.09), in: Capsule())

            if isHidden {
                Text("Hidden")
                    .font(.caption.bold())
                    .foregroundStyle(.orange)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 5)
                    .background(Color.orange.opacity(0.14), in: Capsule())
            }
        }
    }
}
