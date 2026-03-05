import XCTest
@testable import ViewDB

final class PostgresRepositoryPagingTests: XCTestCase {
    func testSelectPagingOrderPrefersIDColumn() {
        let selection = PostgresRepository.selectPagingOrder(
            columns: [
                (name: "id", udtName: "int8"),
                (name: "name", udtName: "text"),
            ],
            primaryKeyColumns: [],
            relationKind: "r"
        )

        XCTAssertEqual(selection.strategy, .keysetID)
        XCTAssertEqual(selection.orderColumn, "id")
    }

    func testSelectPagingOrderUsesSinglePrimaryKeyWhenNoID() {
        let selection = PostgresRepository.selectPagingOrder(
            columns: [
                (name: "event_key", udtName: "uuid"),
                (name: "name", udtName: "text"),
            ],
            primaryKeyColumns: ["event_key"],
            relationKind: "r"
        )

        XCTAssertEqual(selection.strategy, .keysetPrimaryKey)
        XCTAssertEqual(selection.orderColumn, "event_key")
    }

    func testSelectPagingOrderFallsBackToCTIDForPhysicalTable() {
        let selection = PostgresRepository.selectPagingOrder(
            columns: [
                (name: "payload", udtName: "jsonb"),
            ],
            primaryKeyColumns: [],
            relationKind: "r"
        )

        XCTAssertEqual(selection.strategy, .keysetCTID)
        XCTAssertEqual(selection.orderColumn, "ctid")
    }

    func testSelectPagingOrderFallsBackToOffsetForNonPhysicalWithoutSortableKeys() {
        let selection = PostgresRepository.selectPagingOrder(
            columns: [
                (name: "payload", udtName: "jsonb"),
            ],
            primaryKeyColumns: [],
            relationKind: "v"
        )

        XCTAssertEqual(selection.strategy, .offset)
        XCTAssertNil(selection.orderColumn)
    }
}
