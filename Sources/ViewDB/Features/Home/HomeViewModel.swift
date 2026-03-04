import Foundation
import Observation

@MainActor
@Observable
final class HomeViewModel {
    private let discoveryCoordinator: DiscoveryCoordinator
    private let catalogService: any CatalogService

    private(set) var allGroups: [DatabaseGroup] = []
    private(set) var visibleGroups: [DatabaseGroup] = []

    var isLoading = false
    var errorMessage: String?
    var includeSystemDatabases = false
    var searchText = "" {
        didSet {
            scheduleSearchDebounce()
        }
    }

    private var loadTask: Task<Void, Never>?
    private var searchTask: Task<Void, Never>?

    init(discoveryCoordinator: DiscoveryCoordinator, catalogService: any CatalogService) {
        self.discoveryCoordinator = discoveryCoordinator
        self.catalogService = catalogService
    }

    func load() {
        loadTask?.cancel()
        loadTask = Task { [weak self] in
            guard let self else { return }
            await self.loadImpl()
        }
    }

    func loadNow() async {
        await loadImpl()
    }

    func refresh() {
        load()
    }

    func toggleIncludeSystemDatabases() {
        includeSystemDatabases.toggle()
        load()
    }

    private func loadImpl() async {
        isLoading = true
        errorMessage = nil

        let instances = await discoveryCoordinator.discoverInstances()

        let groups = await withTaskGroup(of: DatabaseGroup.self) { group in
            for instance in instances {
                group.addTask { [catalogService, includeSystemDatabases] in
                    do {
                        let databases = try await catalogService.listDatabases(on: instance, includeSystem: includeSystemDatabases)
                        return DatabaseGroup(instance: instance, databases: databases)
                    } catch {
                        return DatabaseGroup(
                            instance: instance,
                            databases: [],
                            warningMessage: PostgresErrorClassifier.message(for: error)
                        )
                    }
                }
            }

            var output: [DatabaseGroup] = []
            for await groupResult in group {
                output.append(groupResult)
            }
            return output.sorted { lhs, rhs in
                if lhs.instance.source == rhs.instance.source {
                    return lhs.instance.displayName.localizedCaseInsensitiveCompare(rhs.instance.displayName) == .orderedAscending
                }
                return lhs.instance.source.rawValue < rhs.instance.source.rawValue
            }
        }

        allGroups = groups
        visibleGroups = applyFilter(groups, query: searchText)
        isLoading = false
    }

    private func scheduleSearchDebounce() {
        searchTask?.cancel()
        let currentQuery = searchText
        searchTask = Task { [weak self] in
            try? await Task.sleep(for: .milliseconds(250))
            guard !Task.isCancelled else { return }
            await MainActor.run {
                guard let self else { return }
                self.visibleGroups = self.applyFilter(self.allGroups, query: currentQuery)
            }
        }
    }

    private func applyFilter(_ groups: [DatabaseGroup], query: String) -> [DatabaseGroup] {
        let normalized = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else {
            return groups
        }

        return groups.compactMap { group in
            let filtered = group.databases.filter { database in
                database.name.localizedCaseInsensitiveContains(normalized)
            }

            if filtered.isEmpty {
                return nil
            }

            return DatabaseGroup(instance: group.instance, databases: filtered, warningMessage: group.warningMessage)
        }
    }
}
