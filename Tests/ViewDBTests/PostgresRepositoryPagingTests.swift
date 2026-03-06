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

    func testMakeColumnTypeNamesPreservesColumnOrder() {
        let types = PostgresRepository.makeColumnTypeNames(
            columns: [
                (name: "id", udtName: "INT8"),
                (name: "payload", udtName: "JSONB"),
                (name: "title", udtName: "text"),
            ]
        )

        XCTAssertEqual(types, ["int8", "jsonb", "text"])
    }

    func testMakePreviewCellValueTruncatesLongPayloads() {
        let value = String(repeating: "a", count: 300)
        let preview = PostgresRepository.makePreviewCellValue(value: value, maxChars: 256)

        XCTAssertTrue(preview.isTruncated)
        XCTAssertEqual(preview.previewText.count, 257)
        XCTAssertTrue(preview.previewText.hasSuffix("…"))
    }

    func testMakePreviewCellValueLeavesShortPayloadUntouched() {
        let preview = PostgresRepository.makePreviewCellValue(value: "hello", maxChars: 256)

        XCTAssertFalse(preview.isTruncated)
        XCTAssertEqual(preview.previewText, "hello")
    }

    func testMakePreviewCellValueLeavesNullMarkerUntouched() {
        let preview = PostgresRepository.makePreviewCellValue(value: "NULL", maxChars: 256)

        XCTAssertFalse(preview.isTruncated)
        XCTAssertEqual(preview.previewText, "NULL")
    }

    func testMakePreviewCellValueTruncatesLargeJSONPayload() {
        let value = """
        {"payload":"\(String(repeating: "x", count: 600))"}
        """
        let preview = PostgresRepository.makePreviewCellValue(value: value, maxChars: 128)

        XCTAssertTrue(preview.isTruncated)
        XCTAssertEqual(preview.previewText.count, 129)
        XCTAssertTrue(preview.previewText.hasSuffix("…"))
    }

    func testMakePreviewCellValueTruncatesHexPayload() {
        let value = "0x" + String(repeating: "ab", count: 300)
        let preview = PostgresRepository.makePreviewCellValue(value: value, maxChars: 128)

        XCTAssertTrue(preview.isTruncated)
        XCTAssertEqual(preview.previewText.count, 129)
        XCTAssertTrue(preview.previewText.hasSuffix("…"))
    }

    func testMakeLookupPlanForNumericIdentity() {
        let plan = PostgresRepository.makeLookupPlan(
            for: .columnValue(column: "id", value: "42", valueType: .numeric)
        )

        XCTAssertEqual(plan, .columnValue(column: "id", literal: "42"))
    }

    func testMakeLookupPlanForTextIdentity() {
        let plan = PostgresRepository.makeLookupPlan(
            for: .columnValue(column: "slug", value: "o'hara", valueType: .textual)
        )

        XCTAssertEqual(plan, .columnValue(column: "slug", literal: "'o''hara'"))
    }

    func testMakeLookupPlanForCTIDIdentity() {
        let plan = PostgresRepository.makeLookupPlan(for: .ctid("(1,2)"))
        XCTAssertEqual(plan, .ctid(literal: "'(1,2)'::tid"))
    }

    func testMakeLookupPlanForOffsetIdentity() {
        let plan = PostgresRepository.makeLookupPlan(for: .offset(9))
        XCTAssertEqual(plan, .offset(9, sort: nil))
    }

    func testExplicitSortAcceptsNumericType() {
        XCTAssertTrue(PostgresRepository.isExplicitlySortableType("int8"))
    }

    func testExplicitSortRejectsJSONB() {
        XCTAssertFalse(PostgresRepository.isExplicitlySortableType("jsonb"))
    }

    func testExplicitSortRejectsArrayType() {
        XCTAssertFalse(PostgresRepository.isExplicitlySortableType("_int4"))
    }
}
