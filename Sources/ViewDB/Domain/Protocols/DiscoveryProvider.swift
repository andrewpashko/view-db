import Foundation

protocol DiscoveryProvider: Sendable {
    var source: InstanceSource { get }
    func discover() async -> [DiscoveredInstance]
}

protocol InstanceLookupService: Sendable {
    func instance(for id: UUID) async -> DiscoveredInstance?
}
