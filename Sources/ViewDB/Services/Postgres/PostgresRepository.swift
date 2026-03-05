import Foundation
import Logging
import PostgresNIO

actor PostgresRepository: CatalogService, QueryService, CredentialService {
    private let sessionPool: PostgresSessionPool
    private let instanceLookup: any InstanceLookupService
    private var databaseToInstance: [UUID: DiscoveredInstance] = [:]

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

    func fetchRows(database: DatabaseRef, table: TableRef, limit: Int, offset: Int) async throws -> RowPage {
        let instance = try await resolveInstance(for: database)
        let safeSchema = quoteIdentifier(table.schema)
        let safeTable = quoteIdentifier(table.name)
        let pageLimit = max(1, limit)
        let offsetValue = max(0, offset)

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let columns = try await self.fetchColumnNames(
                connection: connection,
                logger: logger,
                schema: table.schema,
                table: table.name
            )

            let query = PostgresQuery(unsafeSQL: "SELECT * FROM \(safeSchema).\(safeTable) LIMIT \(pageLimit + 1) OFFSET \(offsetValue)")
            let sequence = try await connection.query(query, logger: logger)
            let collected = try await self.collectRows(sequence: sequence, cap: pageLimit + 1)
            var rows = collected.rows

            let hasNext = rows.count > pageLimit
            if hasNext {
                rows.removeLast()
            }

            return RowPage(columns: columns, rows: rows, limit: pageLimit, offset: offsetValue, hasNext: hasNext)
        }
    }

    func fetchRowCount(database: DatabaseRef, table: TableRef) async throws -> Int {
        let instance = try await resolveInstance(for: database)
        let safeSchema = quoteIdentifier(table.schema)
        let safeTable = quoteIdentifier(table.name)

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let query = PostgresQuery(unsafeSQL: "SELECT COUNT(*) FROM \(safeSchema).\(safeTable)")
            let sequence = try await connection.query(query, logger: logger)
            let rows = try await sequence.collect()
            guard let row = rows.first,
                  let cell = row.first,
                  let count = try? cell.decode(Int64.self) else {
                return 0
            }
            return Int(clamping: count)
        }
    }

    func runReadOnlySQL(database: DatabaseRef, sql: String, limit: Int) async throws -> RowPage {
        try SQLGuard.validateReadOnly(sql)
        let instance = try await resolveInstance(for: database)
        let pageLimit = max(1, limit)

        return try await sessionPool.withConnection(instance: instance, database: database.name) { connection, logger in
            let sequence = try await connection.query(PostgresQuery(unsafeSQL: sql), logger: logger)
            let collected = try await self.collectRows(sequence: sequence, cap: pageLimit + 1)
            var rows = collected.rows

            let hasNext = rows.count > pageLimit
            if hasNext {
                rows.removeLast()
            }

            return RowPage(columns: collected.columns, rows: rows, limit: pageLimit, offset: 0, hasNext: hasNext)
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

    private func fetchColumnNames(
        connection: PostgresConnection,
        logger: Logger,
        schema: String,
        table: String
    ) async throws -> [String] {
        let sequence = try await connection.query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = \(schema)
              AND table_name = \(table)
            ORDER BY ordinal_position
            """,
            logger: logger
        )

        let rows = try await sequence.collect()
        return rows.compactMap { row in
            guard let cell = row.first else { return nil }
            return try? cell.decode(String.self)
        }
    }

    private func collectRows(sequence: PostgresRowSequence, cap: Int) async throws -> (columns: [String], rows: [[String]]) {
        var output: [[String]] = []
        var columns: [String] = []
        output.reserveCapacity(cap)

        for try await row in sequence {
            if output.count >= cap {
                break
            }
            let cells = Array(row)
            if columns.isEmpty {
                columns = cells.map(\.columnName)
            }
            let values = cells.map(Self.displayValue)
            output.append(values)
        }

        return (columns: columns, rows: output)
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
                return ISO8601DateFormatter().string(from: date)
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

    private func quoteIdentifier(_ value: String) -> String {
        "\"\(value.replacingOccurrences(of: "\"", with: "\"\""))\""
    }
}
