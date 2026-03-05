import Foundation
import Logging
import PostgresNIO

actor PostgresRepository: CatalogService, QueryService, CredentialService {
    private struct TableCacheKey: Hashable {
        let databaseID: UUID
        let schema: String
        let table: String
    }

    private enum KeysetValueType: Sendable {
        case numeric
        case textual
        case ctid

        var rowIdentityValueType: RowIdentityValueType? {
            switch self {
            case .numeric:
                return .numeric
            case .textual:
                return .textual
            case .ctid:
                return nil
            }
        }
    }

    private struct TableColumnMeta: Sendable {
        let name: String
        let udtName: String
    }

    private struct TableMetadata: Sendable {
        let columns: [TableColumnMeta]
        let primaryKeyColumns: [String]
        let relationKind: String?
    }

    private struct TablePagingPlan: Sendable {
        let strategy: RowPagingStrategy
        let columns: [String]
        let orderColumn: String?
        let orderType: KeysetValueType?
    }

    private struct QueryExecutionPlan {
        let sql: String
        let hiddenLeadingCursor: Bool
        let orderColumn: String?
        let strategy: RowPagingStrategy
        let orderType: KeysetValueType?
    }

    private struct CollectedRows {
        var columns: [String]
        var rows: [[String]]
        var hiddenCursors: [String]?
    }

    enum FullValueLookupPlan: Equatable {
        case offset(Int)
        case columnValue(column: String, literal: String)
        case ctid(literal: String)
    }

    private let sessionPool: PostgresSessionPool
    private let instanceLookup: any InstanceLookupService
    private var databaseToInstance: [UUID: DiscoveredInstance] = [:]
    private var tableMetadataCache: [TableCacheKey: TableMetadata] = [:]
    private var tablePagingPlanCache: [TableCacheKey: TablePagingPlan] = [:]

    private nonisolated(unsafe) static let iso8601Formatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    init(sessionPool: PostgresSessionPool, instanceLookup: any InstanceLookupService) {
        self.sessionPool = sessionPool
        self.instanceLookup = instanceLookup
    }

    func saveCredentials(for instance: DiscoveredInstance, credentials: ConnectionCredentials) async throws {
        try await sessionPool.saveCredentials(for: instance, credentials: credentials)
    }

    func listDatabases(on instance: DiscoveredInstance, includeSystem: Bool) async throws -> [DatabaseRef] {
        let rows = try await sessionPool.withConnection(instance: instance, database: "postgres") { connection, logger in
            let sequence = try await connection.query(
                "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname",
                logger: logger
            )
            return try await sequence.collect()
        }

        let names: [String] = rows.compactMap { row in
            guard let cell = row.first else { return nil }
            return try? cell.decode(String.self)
        }

        let filteredNames: [String]
        if includeSystem {
            filteredNames = names
        } else {
            let blocked = Set(["postgres", "template0", "template1"])
            filteredNames = names.filter { !blocked.contains($0) }
        }

        let databases = filteredNames.map { name in
            DatabaseRef(instanceID: instance.id, name: name)
        }

        for database in databases {
            databaseToInstance[database.id] = instance
        }

        return databases
    }

    func listTables(on database: DatabaseRef) async throws -> [TableRef] {
        let instance = try await resolveInstance(for: database)

        let rows = try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let sequence = try await connection.query(
                """
                SELECT n.nspname AS schema_name, c.relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, c.relname
                """,
                logger: logger
            )
            return try await sequence.collect()
        }

        invalidateTableCaches(for: database.id)

        return rows.compactMap { row in
            let cells = Array(row)
            guard cells.count >= 2,
                  let schema = try? cells[0].decode(String.self),
                  let name = try? cells[1].decode(String.self) else {
                return nil
            }
            return TableRef(databaseID: database.id, schema: schema, name: name)
        }
    }

    func fetchRows(database: DatabaseRef, table: TableRef, request: RowPageRequest) async throws -> RowPage {
        let instance = try await resolveInstance(for: database)
        let pageLimit = max(1, request.limit)
        let logicalOffset = max(0, request.offset)
        let pagingPlan = try await resolvePagingPlan(
            database: database,
            table: table,
            instance: instance
        )

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let start = CFAbsoluteTimeGetCurrent()
            let queryPlan = Self.makeQueryExecutionPlan(
                table: table,
                pagingPlan: pagingPlan,
                request: request,
                limit: pageLimit
            )

            let sequence = try await connection.query(PostgresQuery(unsafeSQL: queryPlan.sql), logger: logger)
            var collected = try await Self.collectRows(
                sequence: sequence,
                cap: pageLimit + 1,
                hiddenLeadingCursor: queryPlan.hiddenLeadingCursor
            )

            let hasNext: Bool
            switch request.direction {
            case .previous where queryPlan.strategy.usesCursor && request.cursor != nil:
                let hasPreviousPage = collected.rows.count > pageLimit
                if hasPreviousPage {
                    collected.rows.removeLast()
                    collected.hiddenCursors?.removeLast()
                }
                collected.rows.reverse()
                collected.hiddenCursors?.reverse()

                // Going backwards implies a newer page exists (the one we came from).
                hasNext = !collected.rows.isEmpty
            default:
                hasNext = collected.rows.count > pageLimit
                if hasNext {
                    collected.rows.removeLast()
                    collected.hiddenCursors?.removeLast()
                }
            }

            let cursorPair = Self.resolveCursors(
                rows: collected.rows,
                hiddenCursors: collected.hiddenCursors,
                strategy: queryPlan.strategy,
                orderColumn: queryPlan.orderColumn,
                columns: pagingPlan.columns
            )

            let nextCursor: String?
            if queryPlan.strategy.usesCursor {
                nextCursor = hasNext ? cursorPair.next : nil
            } else {
                nextCursor = nil
            }

            let output = RowPage(
                columns: pagingPlan.columns,
                rows: collected.rows,
                limit: pageLimit,
                offset: logicalOffset,
                hasNext: nextCursor != nil || (hasNext && !queryPlan.strategy.usesCursor),
                strategy: queryPlan.strategy,
                orderedByColumn: queryPlan.orderColumn,
                nextCursor: nextCursor,
                previousCursor: cursorPair.previous
            )

            let elapsedMS = Int((CFAbsoluteTimeGetCurrent() - start) * 1000)
            logger.debug(
                "fetchRows completed",
                metadata: [
                    "table": "\(table.fullName)",
                    "strategy": "\(output.strategy.rawValue)",
                    "direction": "\(request.direction.rawValue)",
                    "ms": "\(elapsedMS)",
                ]
            )

            return output
        }
    }

    func fetchRowsPreview(
        database: DatabaseRef,
        table: TableRef,
        request: RowPageRequest,
        previewLimitChars: Int
    ) async throws -> RowPagePreview {
        let instance = try await resolveInstance(for: database)
        let pageLimit = max(1, request.limit)
        let logicalOffset = max(0, request.offset)
        let previewLimit = max(16, previewLimitChars)
        let pagingPlan = try await resolvePagingPlan(
            database: database,
            table: table,
            instance: instance
        )

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let start = CFAbsoluteTimeGetCurrent()
            let queryPlan = Self.makeQueryExecutionPlan(
                table: table,
                pagingPlan: pagingPlan,
                request: request,
                limit: pageLimit
            )

            let sequence = try await connection.query(PostgresQuery(unsafeSQL: queryPlan.sql), logger: logger)
            var collected = try await Self.collectRows(
                sequence: sequence,
                cap: pageLimit + 1,
                hiddenLeadingCursor: queryPlan.hiddenLeadingCursor
            )

            let hasNext: Bool
            switch request.direction {
            case .previous where queryPlan.strategy.usesCursor && request.cursor != nil:
                let hasPreviousPage = collected.rows.count > pageLimit
                if hasPreviousPage {
                    collected.rows.removeLast()
                    collected.hiddenCursors?.removeLast()
                }
                collected.rows.reverse()
                collected.hiddenCursors?.reverse()
                hasNext = !collected.rows.isEmpty
            default:
                hasNext = collected.rows.count > pageLimit
                if hasNext {
                    collected.rows.removeLast()
                    collected.hiddenCursors?.removeLast()
                }
            }

            let cursorPair = Self.resolveCursors(
                rows: collected.rows,
                hiddenCursors: collected.hiddenCursors,
                strategy: queryPlan.strategy,
                orderColumn: queryPlan.orderColumn,
                columns: pagingPlan.columns
            )

            let rows: [TableRowItem] = collected.rows.enumerated().map { rowOffset, row in
                let identity: RowIdentity = Self.makeRowIdentity(
                    strategy: queryPlan.strategy,
                    orderColumn: queryPlan.orderColumn,
                    orderType: queryPlan.orderType,
                    row: row,
                    hiddenCursor: collected.hiddenCursors?[safe: rowOffset],
                    columns: pagingPlan.columns,
                    fallbackOffset: logicalOffset + rowOffset
                )
                let previewValues = row.map { value in
                    Self.makePreviewCellValue(value: value, maxChars: previewLimit)
                }
                return TableRowItem(id: logicalOffset + rowOffset, identity: identity, values: previewValues)
            }

            let nextCursor: String?
            if queryPlan.strategy.usesCursor {
                nextCursor = hasNext ? cursorPair.next : nil
            } else {
                nextCursor = nil
            }

            let output = RowPagePreview(
                columns: pagingPlan.columns,
                rows: rows,
                limit: pageLimit,
                offset: logicalOffset,
                hasNext: nextCursor != nil || (hasNext && !queryPlan.strategy.usesCursor),
                strategy: queryPlan.strategy,
                orderedByColumn: queryPlan.orderColumn,
                nextCursor: nextCursor,
                previousCursor: cursorPair.previous
            )

            let elapsedMS = Int((CFAbsoluteTimeGetCurrent() - start) * 1000)
            logger.debug(
                "fetchRowsPreview completed",
                metadata: [
                    "table": "\(table.fullName)",
                    "strategy": "\(output.strategy.rawValue)",
                    "direction": "\(request.direction.rawValue)",
                    "previewChars": "\(previewLimit)",
                    "rows": "\(rows.count)",
                    "ms": "\(elapsedMS)",
                ]
            )

            return output
        }
    }

    func fetchCellValue(database: DatabaseRef, table: TableRef, rowIdentity: RowIdentity, columnName: String) async throws -> String {
        let instance = try await resolveInstance(for: database)
        let safeSchema = Self.quoteIdentifier(table.schema)
        let safeTable = Self.quoteIdentifier(table.name)
        let safeColumn = Self.quoteIdentifier(columnName)

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let start = CFAbsoluteTimeGetCurrent()
            let lookupPlan = Self.makeLookupPlan(for: rowIdentity)

            let sql: String
            switch lookupPlan {
            case .offset(let offset):
                sql = "SELECT \(safeColumn) FROM \(safeSchema).\(safeTable) LIMIT 1 OFFSET \(max(0, offset))"
            case .columnValue(let column, let literal):
                let safeLookupColumn = Self.quoteIdentifier(column)
                sql = "SELECT \(safeColumn) FROM \(safeSchema).\(safeTable) WHERE \(safeLookupColumn) = \(literal) LIMIT 1"
            case .ctid(let literal):
                sql = "SELECT \(safeColumn) FROM \(safeSchema).\(safeTable) WHERE ctid = \(literal) LIMIT 1"
            }

            let sequence = try await connection.query(PostgresQuery(unsafeSQL: sql), logger: logger)
            let rows = try await sequence.collect()
            guard let firstRow = rows.first,
                  let firstCell = firstRow.first else {
                return ""
            }

            let value = Self.displayValue(firstCell)
            let elapsedMS = Int((CFAbsoluteTimeGetCurrent() - start) * 1000)
            logger.debug(
                "fetchCellValue completed",
                metadata: [
                    "table": "\(table.fullName)",
                    "column": "\(columnName)",
                    "lookup": "\(String(describing: lookupPlan))",
                    "ms": "\(elapsedMS)",
                ]
            )
            return value
        }
    }

    func fetchRowCount(database: DatabaseRef, table: TableRef) async throws -> Int {
        let instance = try await resolveInstance(for: database)
        let safeSchema = Self.quoteIdentifier(table.schema)
        let safeTable = Self.quoteIdentifier(table.name)

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let start = CFAbsoluteTimeGetCurrent()
            let query = PostgresQuery(unsafeSQL: "SELECT COUNT(*) FROM \(safeSchema).\(safeTable)")
            let sequence = try await connection.query(query, logger: logger)
            let rows = try await sequence.collect()
            guard let row = rows.first,
                  let cell = row.first,
                  let count = try? cell.decode(Int64.self) else {
                return 0
            }

            let elapsedMS = Int((CFAbsoluteTimeGetCurrent() - start) * 1000)
            logger.debug(
                "fetchRowCount completed",
                metadata: [
                    "table": "\(table.fullName)",
                    "ms": "\(elapsedMS)",
                ]
            )

            return Int(clamping: count)
        }
    }

    func runReadOnlySQL(database: DatabaseRef, sql: String, limit: Int) async throws -> RowPage {
        try SQLGuard.validateReadOnly(sql)
        let instance = try await resolveInstance(for: database)
        let pageLimit = max(1, limit)

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let sequence = try await connection.query(PostgresQuery(unsafeSQL: sql), logger: logger)
            let collected = try await Self.collectRows(sequence: sequence, cap: pageLimit + 1, hiddenLeadingCursor: false)
            var rows = collected.rows

            let hasNext = rows.count > pageLimit
            if hasNext {
                rows.removeLast()
            }

            return RowPage(
                columns: collected.columns,
                rows: rows,
                limit: pageLimit,
                offset: 0,
                hasNext: hasNext,
                strategy: .offset,
                orderedByColumn: nil,
                nextCursor: nil,
                previousCursor: nil
            )
        }
    }

    private func resolveInstance(for database: DatabaseRef) async throws -> DiscoveredInstance {
        if let cached = databaseToInstance[database.id] {
            return cached
        }
        if let fromLookup = await instanceLookup.instance(for: database.instanceID) {
            databaseToInstance[database.id] = fromLookup
            return fromLookup
        }
        throw AppError.noEndpoint
    }

    private func resolvePagingPlan(
        database: DatabaseRef,
        table: TableRef,
        instance: DiscoveredInstance
    ) async throws -> TablePagingPlan {
        let key = TableCacheKey(databaseID: table.databaseID, schema: table.schema, table: table.name)
        if let cached = tablePagingPlanCache[key] {
            return cached
        }

        let metadata = try await resolveTableMetadata(database: database, table: table, instance: instance, key: key)
        let columnLookup = Dictionary(uniqueKeysWithValues: metadata.columns.map { ($0.name, $0) })
        let columns = metadata.columns.map(\.name)

        let selection = Self.selectPagingOrder(
            columns: metadata.columns.map { (name: $0.name, udtName: $0.udtName) },
            primaryKeyColumns: metadata.primaryKeyColumns,
            relationKind: metadata.relationKind
        )

        if selection.strategy == .keysetID,
           let idColumn = columnLookup["id"],
           let valueType = Self.keysetType(for: idColumn.udtName) {
            let plan = TablePagingPlan(
                strategy: .keysetID,
                columns: columns,
                orderColumn: "id",
                orderType: valueType
            )
            tablePagingPlanCache[key] = plan
            return plan
        }

        if selection.strategy == .keysetPrimaryKey,
           let primaryKey = selection.orderColumn,
           let primaryKeyColumn = columnLookup[primaryKey],
           let valueType = Self.keysetType(for: primaryKeyColumn.udtName) {
            let plan = TablePagingPlan(
                strategy: .keysetPrimaryKey,
                columns: columns,
                orderColumn: primaryKey,
                orderType: valueType
            )
            tablePagingPlanCache[key] = plan
            return plan
        }

        if selection.strategy == .keysetCTID {
            let plan = TablePagingPlan(
                strategy: .keysetCTID,
                columns: columns,
                orderColumn: "ctid",
                orderType: .ctid
            )
            tablePagingPlanCache[key] = plan
            return plan
        }

        let plan = TablePagingPlan(
            strategy: .offset,
            columns: columns,
            orderColumn: nil,
            orderType: nil
        )
        tablePagingPlanCache[key] = plan
        return plan
    }

    private func resolveTableMetadata(
        database: DatabaseRef,
        table: TableRef,
        instance: DiscoveredInstance,
        key: TableCacheKey
    ) async throws -> TableMetadata {
        if let cached = tableMetadataCache[key] {
            return cached
        }

        let metadata = try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let columnsSequence = try await connection.query(
                """
                SELECT column_name, udt_name
                FROM information_schema.columns
                WHERE table_schema = \(table.schema)
                  AND table_name = \(table.name)
                ORDER BY ordinal_position
                """,
                logger: logger
            )
            let columnRows = try await columnsSequence.collect()
            let columns = columnRows.compactMap { row -> TableColumnMeta? in
                let cells = Array(row)
                guard cells.count >= 2,
                      let name = try? cells[0].decode(String.self),
                      let udtName = try? cells[1].decode(String.self) else {
                    return nil
                }
                return TableColumnMeta(name: name, udtName: udtName.lowercased())
            }

            let pkSequence = try await connection.query(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
                WHERE i.indisprimary
                  AND n.nspname = \(table.schema)
                  AND c.relname = \(table.name)
                ORDER BY a.attnum
                """,
                logger: logger
            )
            let pkRows = try await pkSequence.collect()
            let primaryKeys = pkRows.compactMap { row -> String? in
                guard let cell = row.first else { return nil }
                return try? cell.decode(String.self)
            }

            let relkindSequence = try await connection.query(
                """
                SELECT c.relkind::text
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = \(table.schema)
                  AND c.relname = \(table.name)
                LIMIT 1
                """,
                logger: logger
            )
            let relkindRows = try await relkindSequence.collect()
            let relationKind = relkindRows.first.flatMap { row in
                row.first.flatMap { try? $0.decode(String.self) }
            }

            return TableMetadata(columns: columns, primaryKeyColumns: primaryKeys, relationKind: relationKind)
        }

        tableMetadataCache[key] = metadata
        return metadata
    }

    private static func makeQueryExecutionPlan(
        table: TableRef,
        pagingPlan: TablePagingPlan,
        request: RowPageRequest,
        limit: Int
    ) -> QueryExecutionPlan {
        let safeSchema = quoteIdentifier(table.schema)
        let safeTable = quoteIdentifier(table.name)
        let baseTable = "\(safeSchema).\(safeTable)"
        let normalizedOffset = max(0, request.offset)

        switch pagingPlan.strategy {
        case .offset:
            return QueryExecutionPlan(
                sql: offsetQuery(
                    baseTable: baseTable,
                    limit: limit,
                    offset: normalizedOffset,
                    orderedBy: nil
                ),
                hiddenLeadingCursor: false,
                orderColumn: nil,
                strategy: .offset,
                orderType: nil
            )

        case .keysetID, .keysetPrimaryKey:
            guard let orderColumn = pagingPlan.orderColumn,
                  let orderType = pagingPlan.orderType else {
                return QueryExecutionPlan(
                    sql: offsetQuery(baseTable: baseTable, limit: limit, offset: normalizedOffset, orderedBy: nil),
                    hiddenLeadingCursor: false,
                    orderColumn: nil,
                    strategy: .offset,
                    orderType: nil
                )
            }

            let query = keysetOrderedQuery(
                baseTable: baseTable,
                orderColumn: orderColumn,
                orderType: orderType,
                request: request,
                limit: limit
            )
            return QueryExecutionPlan(
                sql: query,
                hiddenLeadingCursor: false,
                orderColumn: orderColumn,
                strategy: pagingPlan.strategy,
                orderType: orderType
            )

        case .keysetCTID:
            let query = ctidOrderedQuery(baseTable: baseTable, request: request, limit: limit)
            return QueryExecutionPlan(
                sql: query,
                hiddenLeadingCursor: true,
                orderColumn: "ctid",
                strategy: .keysetCTID,
                orderType: .ctid
            )
        }
    }

    private static func keysetOrderedQuery(
        baseTable: String,
        orderColumn: String,
        orderType: KeysetValueType,
        request: RowPageRequest,
        limit: Int
    ) -> String {
        let safeOrderColumn = quoteIdentifier(orderColumn)

        if request.direction == .initial && request.offset > 0 {
            return offsetQuery(baseTable: baseTable, limit: limit, offset: request.offset, orderedBy: safeOrderColumn)
        }

        let orderDirection: String = request.direction == .previous ? "DESC" : "ASC"
        var sql = "SELECT * FROM \(baseTable)"

        if request.direction != .initial,
           let cursor = request.cursor,
           let literal = sqlLiteral(for: cursor, type: orderType) {
            let comparator = request.direction == .previous ? "<" : ">"
            sql += " WHERE \(safeOrderColumn) \(comparator) \(literal)"
        } else if request.direction != .initial {
            return offsetQuery(baseTable: baseTable, limit: limit, offset: request.offset, orderedBy: safeOrderColumn)
        }

        sql += " ORDER BY \(safeOrderColumn) \(orderDirection) LIMIT \(limit + 1)"
        return sql
    }

    private static func ctidOrderedQuery(
        baseTable: String,
        request: RowPageRequest,
        limit: Int
    ) -> String {
        if request.direction == .initial && request.offset > 0 {
            return "SELECT ctid::text AS _viewdb_cursor, * FROM \(baseTable) ORDER BY ctid ASC LIMIT \(limit + 1) OFFSET \(request.offset)"
        }

        let orderDirection: String = request.direction == .previous ? "DESC" : "ASC"
        var sql = "SELECT ctid::text AS _viewdb_cursor, * FROM \(baseTable)"

        if request.direction != .initial,
           let cursor = request.cursor,
           let literal = sqlLiteral(for: cursor, type: .ctid) {
            let comparator = request.direction == .previous ? "<" : ">"
            sql += " WHERE ctid \(comparator) \(literal)"
        } else if request.direction != .initial {
            return "SELECT ctid::text AS _viewdb_cursor, * FROM \(baseTable) ORDER BY ctid ASC LIMIT \(limit + 1) OFFSET \(request.offset)"
        }

        sql += " ORDER BY ctid \(orderDirection) LIMIT \(limit + 1)"
        return sql
    }

    private static func offsetQuery(baseTable: String, limit: Int, offset: Int, orderedBy: String?) -> String {
        var sql = "SELECT * FROM \(baseTable)"
        if let orderedBy {
            sql += " ORDER BY \(orderedBy) ASC"
        }
        sql += " LIMIT \(limit + 1) OFFSET \(max(0, offset))"
        return sql
    }

    private static func collectRows(
        sequence: PostgresRowSequence,
        cap: Int,
        hiddenLeadingCursor: Bool
    ) async throws -> CollectedRows {
        var outputRows: [[String]] = []
        var outputColumns: [String] = []
        var hiddenCursors: [String]? = hiddenLeadingCursor ? [] : nil
        outputRows.reserveCapacity(cap)

        for try await row in sequence {
            if outputRows.count >= cap {
                break
            }

            let cells = Array(row)
            guard !cells.isEmpty else {
                continue
            }

            if outputColumns.isEmpty {
                var names = cells.map(\.columnName)
                if hiddenLeadingCursor && !names.isEmpty {
                    names.removeFirst()
                }
                outputColumns = names
            }

            if hiddenLeadingCursor {
                hiddenCursors?.append(Self.displayValue(cells[0]))
                let values = cells.dropFirst().map(Self.displayValue)
                outputRows.append(values)
            } else {
                outputRows.append(cells.map(Self.displayValue))
            }
        }

        return CollectedRows(columns: outputColumns, rows: outputRows, hiddenCursors: hiddenCursors)
    }

    private static func resolveCursors(
        rows: [[String]],
        hiddenCursors: [String]?,
        strategy: RowPagingStrategy,
        orderColumn: String?,
        columns: [String]
    ) -> (previous: String?, next: String?) {
        guard !rows.isEmpty else { return (nil, nil) }

        switch strategy {
        case .keysetCTID:
            guard let hiddenCursors, !hiddenCursors.isEmpty else { return (nil, nil) }
            return (hiddenCursors.first, hiddenCursors.last)
        case .keysetID, .keysetPrimaryKey:
            guard let orderColumn,
                  let index = columns.firstIndex(of: orderColumn) else {
                return (nil, nil)
            }

            let previous = rows.first?[safe: index]
            let next = rows.last?[safe: index]
            return (previous, next)
        case .offset:
            return (nil, nil)
        }
    }

    static func makePreviewCellValue(value: String, maxChars: Int) -> TableCellValue {
        let limit = max(16, maxChars)
        guard let cutoffIndex = value.index(value.startIndex, offsetBy: limit, limitedBy: value.endIndex) else {
            return TableCellValue(previewText: value, isTruncated: false)
        }
        guard cutoffIndex < value.endIndex else {
            return TableCellValue(previewText: value, isTruncated: false)
        }

        let preview = String(value[..<cutoffIndex]) + "…"
        return TableCellValue(previewText: preview, isTruncated: true)
    }

    private static func makeRowIdentity(
        strategy: RowPagingStrategy,
        orderColumn: String?,
        orderType: KeysetValueType?,
        row: [String],
        hiddenCursor: String?,
        columns: [String],
        fallbackOffset: Int
    ) -> RowIdentity {
        switch strategy {
        case .keysetID, .keysetPrimaryKey:
            guard let orderColumn,
                  let orderType,
                  let valueType = orderType.rowIdentityValueType,
                  let index = columns.firstIndex(of: orderColumn),
                  let value = row[safe: index] else {
                return .offset(fallbackOffset)
            }
            return .columnValue(column: orderColumn, value: value, valueType: valueType)
        case .keysetCTID:
            guard let hiddenCursor else {
                return .offset(fallbackOffset)
            }
            return .ctid(hiddenCursor)
        case .offset:
            return .offset(fallbackOffset)
        }
    }

    static func makeLookupPlan(for rowIdentity: RowIdentity) -> FullValueLookupPlan {
        switch rowIdentity {
        case .offset(let value):
            return .offset(max(0, value))
        case .columnValue(let column, let value, let valueType):
            let literal: String
            switch valueType {
            case .numeric:
                let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
                if !trimmed.isEmpty, Double(trimmed) != nil {
                    literal = trimmed
                } else {
                    literal = "0"
                }
            case .textual:
                literal = "'\(escapeSQLLiteral(value))'"
            }
            return .columnValue(column: column, literal: literal)
        case .ctid(let ctid):
            return .ctid(literal: "'\(escapeSQLLiteral(ctid))'::tid")
        }
    }

    private func invalidateTableCaches(for databaseID: UUID) {
        tableMetadataCache = tableMetadataCache.filter { $0.key.databaseID != databaseID }
        tablePagingPlanCache = tablePagingPlanCache.filter { $0.key.databaseID != databaseID }
    }

    static func selectPagingOrder(
        columns: [(name: String, udtName: String)],
        primaryKeyColumns: [String],
        relationKind: String?
    ) -> (strategy: RowPagingStrategy, orderColumn: String?) {
        let columnLookup = Dictionary(uniqueKeysWithValues: columns.map { ($0.name, $0.udtName) })

        if let idType = columnLookup["id"],
           keysetType(for: idType) != nil {
            return (.keysetID, "id")
        }

        if primaryKeyColumns.count == 1,
           let primaryKey = primaryKeyColumns.first,
           let primaryKeyType = columnLookup[primaryKey],
           keysetType(for: primaryKeyType) != nil {
            return (.keysetPrimaryKey, primaryKey)
        }

        if relationKind == "r" {
            return (.keysetCTID, "ctid")
        }

        return (.offset, nil)
    }

    private static func keysetType(for udtName: String) -> KeysetValueType? {
        let normalized = udtName.lowercased()

        let numericTypes: Set<String> = ["int2", "int4", "int8", "float4", "float8", "numeric", "oid"]
        if numericTypes.contains(normalized) {
            return .numeric
        }

        let textualTypes: Set<String> = [
            "uuid",
            "text",
            "varchar",
            "bpchar",
            "name",
            "date",
            "time",
            "timetz",
            "timestamp",
            "timestamptz",
        ]
        if textualTypes.contains(normalized) {
            return .textual
        }

        return nil
    }

    private static func sqlLiteral(for cursor: String, type: KeysetValueType) -> String? {
        switch type {
        case .numeric:
            let trimmed = cursor.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty,
                  Double(trimmed) != nil else {
                return nil
            }
            return trimmed
        case .textual:
            return "'\(escapeSQLLiteral(cursor))'"
        case .ctid:
            return "'\(escapeSQLLiteral(cursor))'::tid"
        }
    }

    private static func escapeSQLLiteral(_ value: String) -> String {
        value.replacingOccurrences(of: "'", with: "''")
    }

    private static func displayValue(_ cell: PostgresCell) -> String {
        guard cell.bytes != nil else {
            return "NULL"
        }

        switch cell.dataType {
        case .bool:
            return (try? cell.decode(Bool.self)).map(String.init) ?? rawValue(cell)
        case .int2:
            return (try? cell.decode(Int16.self)).map(String.init) ?? rawValue(cell)
        case .int4:
            return (try? cell.decode(Int32.self)).map(String.init) ?? rawValue(cell)
        case .int8:
            return (try? cell.decode(Int64.self)).map(String.init) ?? rawValue(cell)
        case .float4:
            return (try? cell.decode(Float.self)).map { "\($0)" } ?? rawValue(cell)
        case .float8, .numeric:
            return (try? cell.decode(Double.self)).map { "\($0)" } ?? rawValue(cell)
        case .uuid:
            return (try? cell.decode(UUID.self)).map { $0.uuidString } ?? rawValue(cell)
        case .date, .time, .timetz, .timestamp, .timestamptz:
            if let date = try? cell.decode(Date.self) {
                return iso8601Formatter.string(from: date)
            }
            return rawValue(cell)
        default:
            if let text = try? cell.decode(String.self) {
                return text
            }
            return rawValue(cell)
        }
    }

    private static func rawValue(_ cell: PostgresCell) -> String {
        guard var bytes = cell.bytes else {
            return "NULL"
        }
        if let string = bytes.readString(length: bytes.readableBytes), !string.isEmpty {
            return string
        }

        bytes.moveReaderIndex(to: 0)
        let hex = bytes.readableBytesView.map { String(format: "%02hhx", $0) }.joined()
        return hex.isEmpty ? "" : "0x\(hex)"
    }

    private static func quoteIdentifier(_ value: String) -> String {
        "\"\(value.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}
