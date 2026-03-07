import Foundation
import Observation

@MainActor
@Observable
final class HomeViewModel {
    private let discoveryCoordinator: DiscoveryCoordinator
    private let catalogService: any CatalogService
    private let userDefaults: UserDefaults
    private static let hiddenDatabaseIDsKey = "home.hiddenDatabaseIDs"

    private(set) var allGroups: [DatabaseGroup] = []
    private(set) var visibleGroups: [DatabaseGroup] = []
    private(set) var hiddenDatabaseIDs: Set<UUID> = []

    var isLoading = false
    var errorMessage: String?
    var showHiddenDatabases = false {
        didSet {
            visibleGroups = applyFilter(allGroups, query: searchText)
        }
    }
    var searchText = "" {
        didSet {
            scheduleSearchDebounce()
        }
    }

    private var loadTask: Task<Void, Never>?
    private var searchTask: Task<Void, Never>?

    init(
        discoveryCoordinator: DiscoveryCoordinator,
        catalogService: any CatalogService,
        userDefaults: UserDefaults = .standard
    ) {
        self.discoveryCoordinator = discoveryCoordinator
        self.catalogService = catalogService
        self.userDefaults = userDefaults
        self.hiddenDatabaseIDs = Self.loadHiddenDatabaseIDs(from: userDefaults, key: Self.hiddenDatabaseIDsKey)
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

    var hiddenDatabaseCount: Int {
        allGroups
            .flatMap(\.databases)
            .filter { hiddenDatabaseIDs.contains($0.id) }
            .count
    }

    func isDatabaseHidden(_ database: DatabaseRef) -> Bool {
        hiddenDatabaseIDs.contains(database.id)
    }

    func hiddenDatabaseCount(for instanceID: UUID) -> Int {
        guard let group = allGroups.first(where: { $0.instance.id == instanceID }) else { return 0 }
        return group.databases.filter { hiddenDatabaseIDs.contains($0.id) }.count
    }

    func toggleDatabaseVisibility(_ database: DatabaseRef) {
        if hiddenDatabaseIDs.contains(database.id) {
            hiddenDatabaseIDs.remove(database.id)
        } else {
            hiddenDatabaseIDs.insert(database.id)
        }
        persistHiddenDatabaseIDs()
        visibleGroups = applyFilter(allGroups, query: searchText)
    }

    private func loadImpl() async {
        isLoading = true
        errorMessage = nil
        defer { isLoading = false }

        let instances = await discoveryCoordinator.discoverInstances()

        let groups = await withTaskGroup(of: DatabaseGroup.self) { group in
            for instance in instances {
                group.addTask { [catalogService] in
                    do {
                        let databases = try await catalogService.listDatabases(on: instance, includeSystem: true)
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
        trimHiddenDatabaseIDs(keeping: groups)
        visibleGroups = applyFilter(groups, query: searchText)
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
        let isSearchActive = !normalized.isEmpty

        return groups.compactMap { group in
            let searchFiltered: [DatabaseRef]
            if isSearchActive {
                searchFiltered = group.databases.filter { database in
                    database.name.localizedCaseInsensitiveContains(normalized)
                }
            } else {
                searchFiltered = group.databases
            }

            let visibilityFiltered = searchFiltered.filter { database in
                showHiddenDatabases || !hiddenDatabaseIDs.contains(database.id)
            }

            if isSearchActive && visibilityFiltered.isEmpty {
                return nil
            }

            return DatabaseGroup(
                instance: group.instance,
                databases: visibilityFiltered,
                warningMessage: group.warningMessage
            )
        }
    }

    private func trimHiddenDatabaseIDs(keeping groups: [DatabaseGroup]) {
        let availableDatabaseIDs = Set(groups.flatMap(\.databases).map(\.id))
        let trimmed = hiddenDatabaseIDs.intersection(availableDatabaseIDs)
        guard trimmed != hiddenDatabaseIDs else { return }
        hiddenDatabaseIDs = trimmed
        persistHiddenDatabaseIDs()
    }

    private func persistHiddenDatabaseIDs() {
        userDefaults.set(hiddenDatabaseIDs.map(\.uuidString), forKey: Self.hiddenDatabaseIDsKey)
    }

    private static func loadHiddenDatabaseIDs(from userDefaults: UserDefaults, key: String) -> Set<UUID> {
        let rawIDs = userDefaults.array(forKey: key) as? [String] ?? []
        return Set(rawIDs.compactMap(UUID.init(uuidString:)))
    }
}
