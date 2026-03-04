import Foundation

struct DatabaseGroup: Identifiable, Hashable, Sendable {
    let id: UUID
    let instance: DiscoveredInstance
    let databases: [DatabaseRef]
    let warningMessage: String?

    init(instance: DiscoveredInstance, databases: [DatabaseRef], warningMessage: String? = nil) {
        self.id = instance.id
        self.instance = instance
        self.databases = databases
        self.warningMessage = warningMessage
    }
}
