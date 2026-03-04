import Foundation

enum SQLGuard {
    private static let allowedStatements: Set<String> = ["select", "with", "explain", "show"]

    static func validateReadOnly(_ sql: String) throws {
        let stripped = stripComments(from: sql).trimmingCharacters(in: .whitespacesAndNewlines)
        guard !stripped.isEmpty else {
            throw AppError.queryRejected("SQL query is empty.")
        }

        let statements = stripped
            .split(separator: ";", omittingEmptySubsequences: true)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }

        guard statements.count == 1 else {
            throw AppError.queryRejected("Only a single read-only statement is allowed.")
        }

        guard let first = statements.first else {
            throw AppError.queryRejected("SQL query is empty.")
        }

        let firstToken = first
            .prefix(while: { !$0.isWhitespace && $0 != "(" })
            .lowercased()

        guard allowedStatements.contains(firstToken) else {
            throw AppError.readOnlyViolation
        }
    }

    private static func stripComments(from sql: String) -> String {
        var value = sql

        // Remove block comments.
        while let start = value.range(of: "/*"), let end = value.range(of: "*/", range: start.lowerBound..<value.endIndex) {
            value.removeSubrange(start.lowerBound..<end.upperBound)
        }

        // Remove -- comments line by line.
        let lines = value.split(separator: "\n", omittingEmptySubsequences: false)
        let cleaned = lines.map { line -> String in
            guard let index = line.range(of: "--") else {
                return String(line)
            }
            return String(line[..<index.lowerBound])
        }

        return cleaned.joined(separator: "\n")
    }
}
