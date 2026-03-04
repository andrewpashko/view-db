import Foundation

actor DiscoveryCoordinator: InstanceLookupService {
    private let providers: [any DiscoveryProvider]
    private var cachedInstances: [UUID: DiscoveredInstance] = [:]

    init(providers: [any DiscoveryProvider]) {
        self.providers = providers
    }

    func discoverInstances() async -> [DiscoveredInstance] {
        let discovered = await withTaskGroup(of: [DiscoveredInstance].self) { group in
            for provider in providers {
                group.addTask {
                    await provider.discover()
                }
            }

            var output: [DiscoveredInstance] = []
            for await partial in group {
                output.append(contentsOf: partial)
            }
            return output
        }

        let deduped = Self.deduplicate(discovered)
            .sorted {
                if $0.source == $1.source {
                    return $0.displayName.localizedCaseInsensitiveCompare($1.displayName) == .orderedAscending
                }
                return $0.source.rawValue < $1.source.rawValue
            }

        cachedInstances = Dictionary(uniqueKeysWithValues: deduped.map { ($0.id, $0) })
        return deduped
    }

    func instance(for id: UUID) async -> DiscoveredInstance? {
        cachedInstances[id]
    }

    static func deduplicate(_ instances: [DiscoveredInstance]) -> [DiscoveredInstance] {
        var seen = Set<String>()
        var output: [DiscoveredInstance] = []

        for instance in instances {
            let key = instance.endpointKey
            guard !seen.contains(key) else { continue }
            seen.insert(key)
            output.append(instance)
        }

        return output
    }
}
