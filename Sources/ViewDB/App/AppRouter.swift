import Foundation
import Observation

@MainActor
@Observable
final class AppRouter {
    var path: [DatabaseRef] = []

    func open(database: DatabaseRef) {
        path.append(database)
    }
}
