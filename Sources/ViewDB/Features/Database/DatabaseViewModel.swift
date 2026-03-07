import Foundation
import Observation
import Logging

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
    private let cellEditingService: any CellEditingService
    private let credentialService: any CredentialService

    private(set) var instance: DiscoveredInstance?

    private(set) var allTables: [TableRef] = []
    private(set) var tableSections: [TableSection] = []
    private(set) var selectedTable: TableRef?

    private(set) var rowPage: RowPage = .empty
    private(set) var sqlRowPage: RowPage = .empty {
        didSet {
            sqlRows = Self.makeRows(from: sqlRowPage)
        }
    }
    private(set) var tableRows: [TableRowItem] = []
    private(set) var sqlRows: [TableRowItem] = []
    private(set) var columnEditDescriptors: [ColumnEditDescriptor] = []
    private(set) var totalRowCount: Int?
    private(set) var activeSort: TableSort?

    private(set) var isLoadingTables = false
    private(set) var isLoadingRows = false
    private(set) var isLoadingCount = false
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
    private var editMetadataTask: Task<Void, Never>?
    private let logger = Logger(label: "com.viewdb.ui.database")
    private let previewLimitChars = 256

    init(
        database: DatabaseRef,
        instanceLookup: any InstanceLookupService,
        catalogService: any CatalogService,
        queryService: any QueryService,
        cellEditingService: any CellEditingService,
        credentialService: any CredentialService
    ) {
        self.database = database
        self.instanceLookup = instanceLookup
        self.catalogService = catalogService
        self.queryService = queryService
        self.cellEditingService = cellEditingService
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
        activeSort = nil
        loadEditMetadata(for: table)
        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .initial,
                offset: 0,
                sort: activeSort
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
                    cursor: cursor,
                    sort: activeSort
                )
            )
            return
        }

        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .next,
                offset: nextOffset,
                sort: activeSort
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
                    cursor: cursor,
                    sort: activeSort
                )
            )
            return
        }

        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .previous,
                offset: newOffset,
                sort: activeSort
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
                offset: 0,
                sort: activeSort
            ),
            refreshCount: true
        )
    }

    func toggleSort(column: String) {
        let nextSort: TableSort?
        if let current = activeSort, current.column == column {
            switch current.direction {
            case .ascending:
                nextSort = TableSort(column: column, direction: .descending)
            case .descending:
                nextSort = nil
            }
        } else {
            nextSort = TableSort(column: column, direction: .ascending)
        }

        activeSort = nextSort
        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .initial,
                offset: 0,
                sort: activeSort
            )
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

    func fetchFullCellValue(rowIdentity: RowIdentity, columnName: String) async -> String? {
        guard let selectedTable else { return nil }

        do {
            return try await queryService.fetchCellValue(
                database: database,
                table: selectedTable,
                rowIdentity: rowIdentity,
                columnName: columnName
            )
        } catch {
            logger.debug("fetchFullCellValue failed: \(String(describing: error))")
            return nil
        }
    }

    func beginCellEdit(row: TableRowItem, columnName: String) async -> String? {
        guard let selectedTable,
              let rowLocator = row.editLocator else {
            return nil
        }

        if let descriptor = editDescriptor(named: columnName),
           !descriptor.isEditable {
            return nil
        }

        do {
            return try await cellEditingService.fetchEditableCellValue(
                database: database,
                table: selectedTable,
                rowLocator: rowLocator,
                columnName: columnName
            )
        } catch {
            let message = PostgresErrorClassifier.message(for: error)
            errorMessage = message
            logger.debug("beginCellEdit failed: \(String(describing: error))")
            return nil
        }
    }

    func saveCellEdit(row: TableRowItem, columnName: String, value: String?) async -> DataGridView.CommitEditOutcome {
        guard let selectedTable,
              let rowLocator = row.editLocator else {
            let message = "The selected row cannot be edited."
            errorMessage = message
            return .failure(message)
        }

        do {
            let savedValue = try await cellEditingService.updateCell(
                database: database,
                table: selectedTable,
                rowLocator: rowLocator,
                columnName: columnName,
                value: value
            )
            patchCell(rowID: row.id, columnName: columnName, value: savedValue)
            refreshCurrentPageAfterEdit()
            return .success(savedValue)
        } catch {
            let message = PostgresErrorClassifier.message(for: error)
            errorMessage = message
            return .failure(message)
        }
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
        defer { isLoadingTables = false }

        let resolvedInstance: DiscoveredInstance?
        if let existing = instance {
            resolvedInstance = existing
        } else {
            resolvedInstance = await instanceLookup.instance(for: database.instanceID)
        }

        guard let resolvedInstance else {
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
                loadEditMetadata(for: match)
                fetchRows(
                    request: RowPageRequest(
                        limit: rowsPerPage,
                        direction: .initial,
                        offset: rowPage.offset,
                        sort: activeSort
                    ),
                    refreshCount: true
                )
            } else if let first = tables.first {
                selectedTable = first
                activeSort = nil
                loadEditMetadata(for: first)
                fetchRows(
                    request: RowPageRequest(
                        limit: rowsPerPage,
                        direction: .initial,
                        offset: 0,
                        sort: activeSort
                    ),
                    refreshCount: true
                )
            } else {
                rowPage = .empty
                columnEditDescriptors = []
                totalRowCount = nil
                isLoadingCount = false
                activeSort = nil
            }
        } catch {
            handle(error: error, instance: resolvedInstance, forSQL: false)
        }
    }

    private func fetchRows(request: RowPageRequest, refreshCount: Bool = false) {
        guard let selectedTable else {
            rowPage = .empty
            columnEditDescriptors = []
            totalRowCount = nil
            isLoadingCount = false
            return
        }

        if refreshCount {
            countTask?.cancel()
            totalRowCount = nil
            isLoadingCount = false
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
            let start = CFAbsoluteTimeGetCurrent()
            let previewPage = try await queryService.fetchRowsPreview(
                database: database,
                table: table,
                request: request,
                previewLimitChars: previewLimitChars
            )
            if Task.isCancelled { return }
            rowPage = Self.makeRowPageMetadata(from: previewPage)
            activeSort = previewPage.sort ?? request.sort
            tableRows = previewPage.rows
            if rowsPerPage != previewPage.limit {
                rowsPerPage = min(previewPage.limit, maxRowsPerPage)
            }
            let elapsedMS = Int((CFAbsoluteTimeGetCurrent() - start) * 1000)
            logger.debug("grid preview updated in \(elapsedMS)ms")

            if refreshCount || totalRowCount == nil {
                fetchRowCount(table: table)
            }
        } catch {
            handle(error: error, instance: instance, forSQL: false)
            totalRowCount = nil
            isLoadingCount = false
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
        isLoadingCount = true
        countTask = Task { [weak self] in
            guard let self else { return }
            defer { self.isLoadingCount = false }

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

    private func loadEditMetadata(for table: TableRef) {
        editMetadataTask?.cancel()
        editMetadataTask = Task { [weak self] in
            guard let self else { return }
            do {
                let descriptors = try await self.cellEditingService.fetchEditMetadata(
                    database: self.database,
                    table: table
                )
                if Task.isCancelled { return }
                guard self.selectedTable?.id == table.id else { return }
                self.columnEditDescriptors = descriptors
            } catch {
                if Task.isCancelled { return }
                guard self.selectedTable?.id == table.id else { return }
                self.columnEditDescriptors = []
                self.logger.debug("edit metadata failed: \(String(describing: error))")
            }
        }
    }

    private func editDescriptor(named columnName: String) -> ColumnEditDescriptor? {
        columnEditDescriptors.first(where: { $0.columnName == columnName })
    }

    private func patchCell(rowID: Int, columnName: String, value: String) {
        guard let rowIndex = tableRows.firstIndex(where: { $0.id == rowID }),
              let columnIndex = rowPage.columns.firstIndex(of: columnName) else {
            return
        }

        var row = tableRows[rowIndex]
        var values = row.values
        guard values.indices.contains(columnIndex) else { return }
        values[columnIndex] = PostgresRepository.makePreviewCellValue(value: value, maxChars: previewLimitChars)
        row = TableRowItem(id: row.id, identity: row.identity, values: values, editLocator: row.editLocator)
        tableRows[rowIndex] = row
    }

    private func refreshCurrentPageAfterEdit() {
        guard selectedTable != nil else { return }
        fetchRows(
            request: RowPageRequest(
                limit: rowsPerPage,
                direction: .initial,
                offset: rowPage.offset,
                sort: activeSort
            )
        )
    }

    private static func makeRowPageMetadata(from preview: RowPagePreview) -> RowPage {
        RowPage(
            columns: preview.columns,
            columnTypeNames: preview.columnTypeNames,
            rows: [],
            limit: preview.limit,
            offset: preview.offset,
            hasNext: preview.hasNext,
            strategy: preview.strategy,
            sort: preview.sort,
            nextCursor: preview.nextCursor,
            previousCursor: preview.previousCursor
        )
    }

    private static func makeRows(from page: RowPage) -> [TableRowItem] {
        page.rows.enumerated().map { offset, row in
            let cellValues = row.map { value in
                TableCellValue(previewText: value, isTruncated: false)
            }
            return TableRowItem(
                id: page.offset + offset,
                identity: .offset(page.offset + offset, sort: page.sort),
                values: cellValues,
                editLocator: nil
            )
        }
    }

    var sortableColumns: Set<String> {
        Set(
            zip(rowPage.columns, rowPage.columnTypeNames)
                .compactMap { column, type in
                    Self.isSortableColumnType(type) ? column : nil
                }
        )
    }

    private static func isSortableColumnType(_ typeName: String) -> Bool {
        let normalized = typeName.lowercased()
        if normalized.hasPrefix("_") {
            return false
        }

        let sortableTypes: Set<String> = [
            "bool",
            "int2", "int4", "int8", "float4", "float8", "numeric", "oid",
            "text", "varchar", "bpchar", "name",
            "uuid",
            "date", "time", "timetz", "timestamp", "timestamptz",
        ]
        return sortableTypes.contains(normalized)
    }
}
