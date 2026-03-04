import Observation
import SwiftUI

@main
struct ViewDBApp: App {
    @State private var router = AppRouter()
    private let environment = AppEnvironment.live()

    var body: some Scene {
        WindowGroup {
            HomeView(router: router, environment: environment)
                .background(Color(nsColor: .windowBackgroundColor))
            .frame(minWidth: 980, minHeight: 680)
        }
    }
}
