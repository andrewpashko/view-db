import SwiftUI

extension View {
    func viewDBGlassCard(interactive: Bool = false, cornerRadius: CGFloat = 14) -> some View {
        modifier(ViewDBGlassCardModifier(interactive: interactive, cornerRadius: cornerRadius))
    }

    @ViewBuilder
    func viewDBGlassButton(prominent: Bool = false) -> some View {
        if #available(macOS 26.0, *) {
            if prominent {
                self.buttonStyle(.glassProminent)
            } else {
                self.buttonStyle(.glass)
            }
        } else if prominent {
            self.buttonStyle(.borderedProminent)
        } else {
            self.buttonStyle(.bordered)
        }
    }
}

private struct ViewDBGlassCardModifier: ViewModifier {
    let interactive: Bool
    let cornerRadius: CGFloat

    func body(content: Content) -> some View {
        if #available(macOS 26.0, *) {
            content
                .glassEffect(
                    interactive ? .regular.interactive() : .regular,
                    in: .rect(cornerRadius: cornerRadius)
                )
        } else {
            content
                .background(.ultraThinMaterial, in: RoundedRectangle(cornerRadius: cornerRadius))
        }
    }
}
