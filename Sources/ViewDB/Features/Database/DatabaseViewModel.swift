import Foundation
import Observation

struct TableSection: Identifiable, Hashable {
    let schema: String
    let tables: [TableRef]

    var id: String { schema }
}

@MainActor
@Observable
final class DatabaseViewModel {
    let database: DatabaseRef

    private let instanceLookup: any InstanceLookupService
    private let catalogService: any CatalogService
    private let queryService: any QueryService
    private let credentialService: any CredentialService

    private(set) var instance: DiscoveredInstance?

    private(set) var allTables: [TableRef] = []
    private(set) var tableSections: [TableSection] = []
    private(set) var selectedTable: TableRef?

    private(set) var rowPage: RowPage = .empty {
        didSet {
            tableRows = Self.makeRows(from: rowPage)
        }
    }
    private(set) var sqlRowPage: RowPage = .empty {
        didSet {
            sqlRows = Self.makeRows(from: sqlRowPage)
        }
    }
    private(set) var tableRows: [TableRowItem] = []
    private(set) var sqlRows: [TableRowItem] = []
    private(set) var totalRowCount: Int?

    private(set) var isLoadingTables = false
    private(set) var isLoadingRows = false
    private(set) var isRunningSQL = false

    private let maxRowsPerPage = 500

    var rowsPerPage = 100
    let rowsPerPageOptions = [50, 100, 200, 500]

    var tableSearch = "" {
        didSet {
            applyTableFilter()
        }
    }

    var errorMessage: String?
    var sqlErrorMessage: String?

    var isSQLSheetPresented = false
    var sqlText = "SELECT now()"

    var credentialPrompt: CredentialPromptState?

    private var rowTask: Task<Void, Never>?
    private var tableTask: Task<Void, Never>?
    private var sqlTask: Task<Void, Never>?
    private var countTask: Task<Void, Never>?

    init(
        database: DatabaseRef,
        instanceLookup: any InstanceLookupService,
        catalogService: any CatalogService,
        queryService: any QueryService,
        credentialService: any CredentialService
    ) {
        self.database = database
        self.instanceLookup = instanceLookup
        self.catalogService = catalogService
        self.queryService = queryService
        self.credentialService = credentialService
    }

    var currentPage: Int {
        (rowPage.offset / pageSize) + 1
    }

    var totalPages: Int {
        guard let totalRowCount else {
            return rowPage.hasNext ? currentPage + 1 : currentPage
        }
        return max(1, Int(ceil(Double(totalRowCount) / Double(pageSize))))
    }

    func load() {
        tableTask?.cancel()
        tableTask = Task { [weak self] in
            guard let self else { return }
            await self.loadTablesImpl()
        }
    }

    func loadNow() async {
        await loadTablesImpl()
    }

    func refresh() {
        load()
    }

    func selectTable(_ table: TableRef) {
        guard selectedTable != table else { return }
        selectedTable = table
        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .initial,
                offset: 0
            ),
            refreshCount: true
        )
    }

    func fetchNextPage() {
        guard rowPage.hasNext else { return }
        let nextOffset = rowPage.offset + pageSize

        if rowPage.strategy.usesCursor {
            guard let cursor = rowPage.nextCursor else { return }
            fetchRows(
                request: RowPageRequest(
                    limit: rowsPerPage,
                    direction: .next,
                    offset: nextOffset,
                    cursor: cursor
                )
            )
            return
        }

        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .next,
                offset: nextOffset
            )
        )
    }

    func fetchPreviousPage() {
        guard selectedTable != nil else { return }
        let newOffset = max(0, rowPage.offset - pageSize)
        if rowPage.strategy.usesCursor, let cursor = rowPage.previousCursor {
            fetchRows(
                request: RowPageRequest(
                    limit: rowsPerPage,
                    direction: .previous,
                    offset: newOffset,
                    cursor: cursor
                )
            )
            return
        }

        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .previous,
                offset: newOffset
            )
        )
    }

    func updateRowsPerPage(_ value: Int) {
        let normalized = min(max(1, value), maxRowsPerPage)
        guard rowsPerPage != normalized else { return }
        rowsPerPage = normalized
        fetchRows(
            request: RowPageRequest(
                limit: normalized,
                direction: .initial,
                offset: 0
            ),
            refreshCount: true
        )
    }

    func openSQLSheet() {
        sqlErrorMessage = nil
        isSQLSheetPresented = true
    }

    func runSQL() {
        sqlTask?.cancel()
        sqlTask = Task { [weak self] in
            guard let self else { return }
            await self.runSQLImpl()
        }
    }

    func runSQLNow() async {
        await runSQLImpl()
    }

    func submitCredentials(_ credentials: ConnectionCredentials) {
        guard let instance else { return }

        Task { [weak self] in
            guard let self else { return }
            do {
                try await self.credentialService.saveCredentials(for: instance, credentials: credentials)
                await MainActor.run {
                    self.credentialPrompt = nil
                    self.errorMessage = nil
                    self.sqlErrorMessage = nil
                    self.load()
                }
            } catch {
                await MainActor.run {
                    self.errorMessage = PostgresErrorClassifier.message(for: error)
                }
            }
        }
    }

    private func loadTablesImpl() async {
        isLoadingTables = true
        errorMessage = nil

        let resolvedInstance: DiscoveredInstance?
        if let existing = instance {
            resolvedInstance = existing
        } else {
            resolvedInstance = await instanceLookup.instance(for: database.instanceID)
        }

        guard let resolvedInstance else {
            isLoadingTables = false
            errorMessage = AppError.noEndpoint.errorDescription
            return
        }

        instance = resolvedInstance

        do {
            let tables = try await catalogService.listTables(on: database)
            allTables = tables
            applyTableFilter()

            if let selectedTable,
               let match = tables.first(where: { $0.id == selectedTable.id }) {
                self.selectedTable = match
                fetchRows(
                    request: RowPageRequest(
                        limit: rowsPerPage,
                        direction: .initial,
                        offset: rowPage.offset
                    ),
                    refreshCount: true
                )
            } else if let first = tables.first {
                selectedTable = first
                fetchRows(
                    request: RowPageRequest(
                        limit: rowsPerPage,
                        direction: .initial,
                        offset: 0
                    ),
                    refreshCount: true
                )
            } else {
                rowPage = .empty
                totalRowCount = nil
            }
        } catch {
            handle(error: error, instance: resolvedInstance, forSQL: false)
        }

        isLoadingTables = false
    }

    private func fetchRows(request: RowPageRequest, refreshCount: Bool = false) {
        guard let selectedTable else {
            rowPage = .empty
            totalRowCount = nil
            return
        }

        if refreshCount {
            countTask?.cancel()
            totalRowCount = nil
        }

        rowTask?.cancel()
        rowTask = Task { [weak self] in
            guard let self else { return }
            await self.fetchRowsImpl(table: selectedTable, request: request, refreshCount: refreshCount)
        }
    }

    private func fetchRowsImpl(table: TableRef, request: RowPageRequest, refreshCount: Bool) async {
        isLoadingRows = true
        errorMessage = nil
        defer { isLoadingRows = false }

        do {
            let page = try await queryService.fetchRows(database: database, table: table, request: request)
            if Task.isCancelled { return }
            rowPage = page
            if rowsPerPage != page.limit {
                rowsPerPage = min(page.limit, maxRowsPerPage)
            }

            if refreshCount || totalRowCount == nil {
                fetchRowCount(table: table)
            }
        } catch {
            handle(error: error, instance: instance, forSQL: false)
            totalRowCount = nil
        }
    }

    private func runSQLImpl() async {
        isRunningSQL = true
        sqlErrorMessage = nil

        do {
            let result = try await queryService.runReadOnlySQL(database: database, sql: sqlText, limit: 200)
            if Task.isCancelled { return }
            sqlRowPage = result
        } catch {
            handle(error: error, instance: instance, forSQL: true)
        }

        isRunningSQL = false
    }

    private func handle(error: any Error, instance: DiscoveredInstance?, forSQL: Bool) {
        let message = PostgresErrorClassifier.message(for: error)
        if forSQL {
            sqlErrorMessage = message
        } else {
            errorMessage = message
        }

        if let instance,
           let prompt = PostgresErrorClassifier.credentialPromptState(for: error, instance: instance) {
            credentialPrompt = prompt
        }
    }

    private func applyTableFilter() {
        let normalized = tableSearch.trimmingCharacters(in: .whitespacesAndNewlines)

        let filtered: [TableRef]
        if normalized.isEmpty {
            filtered = allTables
        } else {
            filtered = allTables.filter { table in
                table.name.localizedCaseInsensitiveContains(normalized) ||
                table.schema.localizedCaseInsensitiveContains(normalized)
            }
        }

        let grouped = Dictionary(grouping: filtered, by: \.schema)
        tableSections = grouped
            .map { schema, tables in
                TableSection(schema: schema, tables: tables.sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending })
            }
            .sorted { $0.schema.localizedCaseInsensitiveCompare($1.schema) == .orderedAscending }
    }

    private func fetchRowCount(table: TableRef) {
        countTask?.cancel()
        countTask = Task { [weak self] in
            guard let self else { return }

            do {
                let count = try await self.queryService.fetchRowCount(database: self.database, table: table)
                if Task.isCancelled { return }
                guard self.selectedTable?.id == table.id else { return }
                self.totalRowCount = max(0, count)
            } catch {
                if Task.isCancelled { return }
                guard self.selectedTable?.id == table.id else { return }
                self.totalRowCount = nil
            }
        }
    }

    private var pageSize: Int {
        let fallback = max(1, rowsPerPage)
        return max(1, rowPage.limit == 0 ? fallback : rowPage.limit)
    }

    private static func makeRows(from page: RowPage) -> [TableRowItem] {
        page.rows.enumerated().map { offset, row in
            TableRowItem(id: page.offset + offset, values: row)
        }
    }
}
