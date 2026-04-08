import SwiftUI

struct ExpandableSearchField: View {
    @Binding var text: String
    var isFocused: FocusState<Bool>.Binding

    @State private var isExpanded = false

    private let collapsedSize: CGFloat = 36
    private let expandedWidth: CGFloat = 220
    private let height: CGFloat = 36
    private let cornerRadius: CGFloat = 18

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)
                .font(.system(size: 14, weight: .semibold))
                .frame(width: 16, height: 16)

            if isExpanded {
                TextField("Search table…", text: $text)
                    .textFieldStyle(.plain)
                    .focused(isFocused)
                    .onSubmit { isFocused.wrappedValue = false }
                    .transition(.opacity)

                if !text.isEmpty {
                    Button {
                        text = ""
                        isFocused.wrappedValue = true
                    } label: {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundStyle(.secondary)
                            .font(.system(size: 14))
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel("Clear search")
                    .transition(.opacity.combined(with: .scale))
                }
            }
        }
        .padding(.horizontal, isExpanded ? 14 : 0)
        .frame(width: isExpanded ? expandedWidth : collapsedSize, height: height)
        .viewDBGlassCard(interactive: true, cornerRadius: cornerRadius)
        .overlay(
            RoundedRectangle(cornerRadius: cornerRadius)
                .strokeBorder(Color.secondary.opacity(0.18), lineWidth: 1)
        )
        .contentShape(Rectangle())
        .onTapGesture {
            guard !isExpanded else { return }
            withAnimation(.spring(response: 0.35, dampingFraction: 0.85)) {
                isExpanded = true
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
                isFocused.wrappedValue = true
            }
        }
        .onChange(of: isFocused.wrappedValue) { _, focused in
            if !focused && text.isEmpty {
                withAnimation(.spring(response: 0.35, dampingFraction: 0.85)) {
                    isExpanded = false
                }
            }
        }
        .onChange(of: text) { _, newValue in
            if newValue.isEmpty && !isFocused.wrappedValue {
                withAnimation(.spring(response: 0.35, dampingFraction: 0.85)) {
                    isExpanded = false
                }
            }
        }
        .accessibilityLabel(isExpanded ? "Search table" : "Open table search")
    }
}
