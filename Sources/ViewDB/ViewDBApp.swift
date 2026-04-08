import AppKit
import Observation
import SwiftUI

@main
struct ViewDBApp: App {
    @State private var router = AppRouter()
    private let environment = AppEnvironment.live()

    init() {
        // SPM executables launched from Xcode lack a .app bundle, so macOS
        // may not grant them regular (foreground) activation policy. Without
        // it the process cannot receive keyboard input.
        if Bundle.main.bundleURL.pathExtension != "app" {
            NSApplication.shared.setActivationPolicy(.regular)
            NSApplication.shared.activate()
        }
    }

    var body: some Scene {
        WindowGroup {
            HomeView(router: router, environment: environment)
                .background(Color(nsColor: .windowBackgroundColor))
            .frame(minWidth: 980, minHeight: 680)
        }
    }
}
