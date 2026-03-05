import XCTest
@testable import ViewDB

private struct StaticDiscoveryProvider: DiscoveryProvider {
    let source: InstanceSource
    let instances: [DiscoveredInstance]

    func discover() async -> [DiscoveredInstance] {
        instances
    }
}

private actor MockCatalogService: CatalogService {
    let databaseNames: [String]
    let tables: [TableRef]
    var listTablesDelayMS: UInt64 = 0

    init(databaseNames: [String], tables: [TableRef] = []) {
        self.databaseNames = databaseNames
        self.tables = tables
    }

    func listDatabases(on instance: DiscoveredInstance, includeSystem: Bool) async throws -> [DatabaseRef] {
        let names: [String]
        if includeSystem {
            names = databaseNames
        } else {
            names = databaseNames.filter { !["postgres", "template0", "template1"].contains($0) }
        }
        return names.map { DatabaseRef(instanceID: instance.id, name: $0) }
    }

    func listTables(on database: DatabaseRef) async throws -> [TableRef] {
        if listTablesDelayMS > 0 {
            try? await Task.sleep(for: .milliseconds(Int(listTablesDelayMS)))
        }
        return tables
    }

    func setListTablesDelay(milliseconds: UInt64) {
        listTablesDelayMS = milliseconds
    }
}

private actor MockInstanceLookupService: InstanceLookupService {
    let instance: DiscoveredInstance

    init(instance: DiscoveredInstance) {
        self.instance = instance
    }

    func instance(for id: UUID) async -> DiscoveredInstance? {
        id == instance.id ? instance : nil
    }
}

private actor MockQueryService: QueryService {
    private(set) var fetchedTableNames: [String] = []
    private(set) var fetchedRequests: [RowPageRequest] = []
    var rowCountDelayMS: UInt64 = 0

    private let initialPage: RowPage
    private let nextPage: RowPage
    private let previousPage: RowPage
    private let rowCountValue: Int

    init(
        initialPage: RowPage = RowPage(
            columns: ["id", "name"],
            rows: [["1", "demo"]],
            limit: 100,
            offset: 0,
            hasNext: false,
            strategy: .offset,
            orderedByColumn: nil,
            nextCursor: nil,
            previousCursor: nil
        ),
        nextPage: RowPage? = nil,
        previousPage: RowPage? = nil,
        rowCountValue: Int = 1
    ) {
        self.initialPage = initialPage
        self.nextPage = nextPage ?? initialPage
        self.previousPage = previousPage ?? initialPage
        self.rowCountValue = rowCountValue
    }

    func fetchRows(database: DatabaseRef, table: TableRef, request: RowPageRequest) async throws -> RowPage {
        fetchedTableNames.append(table.name)
        fetchedRequests.append(request)
        switch request.direction {
        case .initial:
            return initialPage
        case .next:
            return nextPage
        case .previous:
            return previousPage
        }
    }

    func fetchRowCount(database: DatabaseRef, table: TableRef) async throws -> Int {
        if rowCountDelayMS > 0 {
            try? await Task.sleep(for: .milliseconds(Int(rowCountDelayMS)))
        }
        return rowCountValue
    }

    func runReadOnlySQL(database: DatabaseRef, sql: String, limit: Int) async throws -> RowPage {
        RowPage(
            columns: ["value"],
            rows: [["1"]],
            limit: limit,
            offset: 0,
            hasNext: false,
            strategy: .offset,
            orderedByColumn: nil,
            nextCursor: nil,
            previousCursor: nil
        )
    }

    func lastFetchedTableName() -> String? {
        fetchedTableNames.last
    }

    func fetchCount() -> Int {
        fetchedTableNames.count
    }

    func requests() -> [RowPageRequest] {
        fetchedRequests
    }

    func setRowCountDelay(milliseconds: UInt64) {
        rowCountDelayMS = milliseconds
    }
}

private actor MockCredentialService: CredentialService {
    func saveCredentials(for instance: DiscoveredInstance, credentials: ConnectionCredentials) async throws {}
}

@MainActor
final class HomeAndDatabaseViewModelTests: XCTestCase {
    private func isolatedDefaults() -> UserDefaults {
        let suiteName = "HomeViewModelTests.\(UUID().uuidString)"
        guard let defaults = UserDefaults(suiteName: suiteName) else {
            fatalError("Unable to create isolated UserDefaults")
        }
        defaults.removePersistentDomain(forName: suiteName)
        return defaults
    }

    func testHomeViewModelLoadsAndFilters() async {
        let instance = DiscoveredInstance(
            source: .brew,
            displayName: "Brew PG",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )

        let provider = StaticDiscoveryProvider(source: .brew, instances: [instance])
        let coordinator = DiscoveryCoordinator(providers: [provider])
        let catalog = MockCatalogService(databaseNames: ["postgres", "app_db"])
        let defaults = isolatedDefaults()

        let vm = HomeViewModel(
            discoveryCoordinator: coordinator,
            catalogService: catalog,
            userDefaults: defaults
        )
        await vm.loadNow()

        XCTAssertEqual(vm.visibleGroups.count, 1)
        XCTAssertEqual(vm.visibleGroups[0].databases.map(\.name), ["postgres", "app_db"])

        vm.searchText = "app"
        try? await Task.sleep(for: .milliseconds(300))

        XCTAssertEqual(vm.visibleGroups.count, 1)
        XCTAssertEqual(vm.visibleGroups[0].databases.map(\.name), ["app_db"])
    }

    func testHomeViewModelHideAndShowDatabase() async {
        let instance = DiscoveredInstance(
            source: .brew,
            displayName: "Brew PG",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )

        let provider = StaticDiscoveryProvider(source: .brew, instances: [instance])
        let coordinator = DiscoveryCoordinator(providers: [provider])
        let catalog = MockCatalogService(databaseNames: ["postgres", "app_db"])
        let defaults = isolatedDefaults()

        let vm = HomeViewModel(
            discoveryCoordinator: coordinator,
            catalogService: catalog,
            userDefaults: defaults
        )
        await vm.loadNow()

        guard let postgres = vm.visibleGroups.first?.databases.first(where: { $0.name == "postgres" }) else {
            XCTFail("Expected postgres database")
            return
        }

        vm.toggleDatabaseVisibility(postgres)
        XCTAssertEqual(vm.hiddenDatabaseCount, 1)
        XCTAssertEqual(vm.visibleGroups[0].databases.map(\.name), ["app_db"])

        vm.showHiddenDatabases = true
        XCTAssertEqual(vm.visibleGroups[0].databases.map(\.name), ["postgres", "app_db"])
    }

    func testHomeViewModelPersistsHiddenDatabases() async {
        let instance = DiscoveredInstance(
            source: .brew,
            displayName: "Brew PG",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )

        let provider = StaticDiscoveryProvider(source: .brew, instances: [instance])
        let coordinator = DiscoveryCoordinator(providers: [provider])
        let catalog = MockCatalogService(databaseNames: ["postgres", "app_db"])

        let suiteName = "HomeViewModelPersistenceTests.\(UUID().uuidString)"
        guard let defaults = UserDefaults(suiteName: suiteName) else {
            XCTFail("Unable to create isolated UserDefaults")
            return
        }
        defaults.removePersistentDomain(forName: suiteName)
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let vm = HomeViewModel(
            discoveryCoordinator: coordinator,
            catalogService: catalog,
            userDefaults: defaults
        )
        await vm.loadNow()

        guard let postgres = vm.visibleGroups.first?.databases.first(where: { $0.name == "postgres" }) else {
            XCTFail("Expected postgres database")
            return
        }
        vm.toggleDatabaseVisibility(postgres)
        XCTAssertEqual(vm.hiddenDatabaseCount, 1)

        let reloadedVM = HomeViewModel(
            discoveryCoordinator: coordinator,
            catalogService: catalog,
            userDefaults: defaults
        )
        await reloadedVM.loadNow()

        XCTAssertEqual(reloadedVM.hiddenDatabaseCount, 1)
        XCTAssertEqual(reloadedVM.visibleGroups[0].databases.map(\.name), ["app_db"])
    }

    func testDatabaseViewModelAutoSelectsFirstTableAndFetchesRows() async {
        let instance = DiscoveredInstance(
            source: .brew,
            displayName: "Brew PG",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )

        let database = DatabaseRef(instanceID: instance.id, name: "app_db")
        let tables = [
            TableRef(databaseID: database.id, schema: "public", name: "orders"),
            TableRef(databaseID: database.id, schema: "public", name: "users"),
        ]

        let lookup = MockInstanceLookupService(instance: instance)
        let catalog = MockCatalogService(databaseNames: [database.name], tables: tables)
        let query = MockQueryService()
        let credentials = MockCredentialService()

        let vm = DatabaseViewModel(
            database: database,
            instanceLookup: lookup,
            catalogService: catalog,
            queryService: query,
            credentialService: credentials
        )

        await vm.loadNow()
        try? await Task.sleep(for: .milliseconds(150))

        XCTAssertEqual(vm.selectedTable?.name, "orders")
        let initialFetchCount = await query.fetchCount()
        XCTAssertEqual(initialFetchCount, 1)

        vm.selectTable(tables[1])
        try? await Task.sleep(for: .milliseconds(150))

        let lastFetchedTable = await query.lastFetchedTableName()
        XCTAssertEqual(lastFetchedTable, "users")
    }

    func testDatabaseViewModelUsesCursorForKeysetNextAndPrevious() async {
        let instance = DiscoveredInstance(
            source: .brew,
            displayName: "Brew PG",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )

        let database = DatabaseRef(instanceID: instance.id, name: "app_db")
        let table = TableRef(databaseID: database.id, schema: "public", name: "events")
        let tables = [table]

        let firstPage = RowPage(
            columns: ["id", "name"],
            rows: [["1", "a"], ["2", "b"]],
            limit: 2,
            offset: 0,
            hasNext: true,
            strategy: .keysetID,
            orderedByColumn: "id",
            nextCursor: "2",
            previousCursor: "1"
        )
        let secondPage = RowPage(
            columns: ["id", "name"],
            rows: [["3", "c"], ["4", "d"]],
            limit: 2,
            offset: 2,
            hasNext: true,
            strategy: .keysetID,
            orderedByColumn: "id",
            nextCursor: "4",
            previousCursor: "3"
        )

        let lookup = MockInstanceLookupService(instance: instance)
        let catalog = MockCatalogService(databaseNames: [database.name], tables: tables)
        let query = MockQueryService(
            initialPage: firstPage,
            nextPage: secondPage,
            previousPage: firstPage,
            rowCountValue: 4
        )
        let credentials = MockCredentialService()

        let vm = DatabaseViewModel(
            database: database,
            instanceLookup: lookup,
            catalogService: catalog,
            queryService: query,
            credentialService: credentials
        )

        await vm.loadNow()
        try? await Task.sleep(for: .milliseconds(120))

        vm.fetchNextPage()
        try? await Task.sleep(for: .milliseconds(120))

        vm.fetchPreviousPage()
        try? await Task.sleep(for: .milliseconds(120))

        let requests = await query.requests()
        XCTAssertEqual(requests.count, 3)
        XCTAssertEqual(requests[0].direction, .initial)

        XCTAssertEqual(requests[1].direction, .next)
        XCTAssertEqual(requests[1].cursor, "2")
        XCTAssertEqual(requests[1].offset, 2)

        XCTAssertEqual(requests[2].direction, .previous)
        XCTAssertEqual(requests[2].cursor, "3")
        XCTAssertEqual(requests[2].offset, 0)
    }

    func testDatabaseViewModelLoadsRowsBeforeLazyCountCompletes() async {
        let instance = DiscoveredInstance(
            source: .brew,
            displayName: "Brew PG",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )

        let database = DatabaseRef(instanceID: instance.id, name: "app_db")
        let table = TableRef(databaseID: database.id, schema: "public", name: "events")
        let tables = [table]

        let initialPage = RowPage(
            columns: ["id", "name"],
            rows: [["1", "a"]],
            limit: 1,
            offset: 0,
            hasNext: false,
            strategy: .offset,
            orderedByColumn: nil,
            nextCursor: nil,
            previousCursor: nil
        )

        let lookup = MockInstanceLookupService(instance: instance)
        let catalog = MockCatalogService(databaseNames: [database.name], tables: tables)
        let query = MockQueryService(initialPage: initialPage, rowCountValue: 5_109)
        await query.setRowCountDelay(milliseconds: 400)
        let credentials = MockCredentialService()

        let vm = DatabaseViewModel(
            database: database,
            instanceLookup: lookup,
            catalogService: catalog,
            queryService: query,
            credentialService: credentials
        )

        await vm.loadNow()
        try? await Task.sleep(for: .milliseconds(80))

        XCTAssertEqual(vm.tableRows.count, 1)
        XCTAssertNil(vm.totalRowCount)

        try? await Task.sleep(for: .milliseconds(450))
        XCTAssertEqual(vm.totalRowCount, 5_109)
    }

    func testDatabaseViewModelTracksTableLoadingStateDuringLoad() async {
        let instance = DiscoveredInstance(
            source: .brew,
            displayName: "Brew PG",
            host: "localhost",
            port: 5432,
            socketPath: nil
        )

        let database = DatabaseRef(instanceID: instance.id, name: "app_db")
        let table = TableRef(databaseID: database.id, schema: "public", name: "events")
        let tables = [table]

        let lookup = MockInstanceLookupService(instance: instance)
        let catalog = MockCatalogService(databaseNames: [database.name], tables: tables)
        await catalog.setListTablesDelay(milliseconds: 350)
        let query = MockQueryService()
        let credentials = MockCredentialService()

        let vm = DatabaseViewModel(
            database: database,
            instanceLookup: lookup,
            catalogService: catalog,
            queryService: query,
            credentialService: credentials
        )

        let loadTask = Task {
            await vm.loadNow()
        }

        try? await Task.sleep(for: .milliseconds(50))
        XCTAssertTrue(vm.isLoadingTables)

        await loadTask.value
        XCTAssertFalse(vm.isLoadingTables)
    }
}
