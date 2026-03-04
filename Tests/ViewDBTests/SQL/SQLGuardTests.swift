import XCTest
@testable import ViewDB

final class SQLGuardTests: XCTestCase {
    func testAllowsReadOnlyStatements() throws {
        XCTAssertNoThrow(try SQLGuard.validateReadOnly("SELECT * FROM users"))
        XCTAssertNoThrow(try SQLGuard.validateReadOnly("WITH cte AS (SELECT 1) SELECT * FROM cte"))
        XCTAssertNoThrow(try SQLGuard.validateReadOnly("EXPLAIN SELECT * FROM users"))
        XCTAssertNoThrow(try SQLGuard.validateReadOnly("SHOW statement_timeout"))
    }

    func testRejectsWriteStatements() {
        XCTAssertThrowsError(try SQLGuard.validateReadOnly("UPDATE users SET name = 'x'"))
        XCTAssertThrowsError(try SQLGuard.validateReadOnly("INSERT INTO users(id) VALUES (1)"))
        XCTAssertThrowsError(try SQLGuard.validateReadOnly("CREATE TABLE a(id int)"))
    }

    func testRejectsMultipleStatements() {
        XCTAssertThrowsError(try SQLGuard.validateReadOnly("SELECT 1; SELECT 2"))
    }
}
