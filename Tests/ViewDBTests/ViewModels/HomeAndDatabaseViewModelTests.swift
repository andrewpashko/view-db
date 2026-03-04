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
        tables
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

    func fetchRows(database: DatabaseRef, table: TableRef, limit: Int, offset: Int) async throws -> RowPage {
        fetchedTableNames.append(table.name)
        return RowPage(
            columns: ["id", "name"],
            rows: [["1", "demo"]],
            limit: limit,
            offset: offset,
            hasNext: false
        )
    }

    func runReadOnlySQL(database: DatabaseRef, sql: String, limit: Int) async throws -> RowPage {
        RowPage(columns: ["value"], rows: [["1"]], limit: limit, offset: 0, hasNext: false)
    }

    func lastFetchedTableName() -> String? {
        fetchedTableNames.last
    }

    func fetchCount() -> Int {
        fetchedTableNames.count
    }
}

private actor MockCredentialService: CredentialService {
    func saveCredentials(for instance: DiscoveredInstance, credentials: ConnectionCredentials) async throws {}
}

@MainActor
final class HomeAndDatabaseViewModelTests: XCTestCase {
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

        let vm = HomeViewModel(discoveryCoordinator: coordinator, catalogService: catalog)
        await vm.loadNow()

        XCTAssertEqual(vm.visibleGroups.count, 1)
        XCTAssertEqual(vm.visibleGroups[0].databases.map(\.name), ["app_db"])

        vm.searchText = "app"
        try? await Task.sleep(for: .milliseconds(300))

        XCTAssertEqual(vm.visibleGroups.count, 1)
        XCTAssertEqual(vm.visibleGroups[0].databases.map(\.name), ["app_db"])
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
}
