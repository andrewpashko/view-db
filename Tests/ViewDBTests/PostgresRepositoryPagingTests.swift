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

    func testMakeRowEditLocatorUsesPrimaryKeyValues() {
        let locator = PostgresRepository.makeRowEditLocator(
            row: ["42", "demo"],
            columns: [
                (name: "id", udtName: "int8"),
                (name: "name", udtName: "text"),
            ],
            primaryKeyColumns: ["id"],
            relationKind: "r"
        )

        XCTAssertEqual(
            locator,
            RowEditLocator(keys: [
                RowEditKey(columnName: "id", value: "42", typeName: "int8"),
            ])
        )
    }

    func testMakeRowEditLocatorSupportsCompositePrimaryKeys() {
        let locator = PostgresRepository.makeRowEditLocator(
            row: ["eu", "42", "demo"],
            columns: [
                (name: "region", udtName: "text"),
                (name: "id", udtName: "int8"),
                (name: "name", udtName: "text"),
            ],
            primaryKeyColumns: ["region", "id"],
            relationKind: "r"
        )

        XCTAssertEqual(
            locator,
            RowEditLocator(keys: [
                RowEditKey(columnName: "region", value: "eu", typeName: "text"),
                RowEditKey(columnName: "id", value: "42", typeName: "int8"),
            ])
        )
    }

    func testMakeRowEditLocatorRejectsViews() {
        let locator = PostgresRepository.makeRowEditLocator(
            row: ["42"],
            columns: [(name: "id", udtName: "int8")],
            primaryKeyColumns: ["id"],
            relationKind: "v"
        )

        XCTAssertNil(locator)
    }

    func testMakeColumnEditDescriptorUsesEnumPicker() {
        let descriptor = PostgresRepository.makeColumnEditDescriptor(
            columnName: "status",
            typeName: "order_status",
            enumOptions: ["draft", "paid"],
            isNullable: false,
            hasDefaultValue: true,
            isGenerated: false,
            isUpdatable: true,
            relationKind: "r"
        )

        XCTAssertEqual(descriptor.editorKind, .enumeration(options: ["draft", "paid"]))
        XCTAssertTrue(descriptor.isEditable)
    }

    func testMakeColumnEditDescriptorRejectsGeneratedColumns() {
        let descriptor = PostgresRepository.makeColumnEditDescriptor(
            columnName: "slug",
            typeName: "text",
            isNullable: false,
            hasDefaultValue: false,
            isGenerated: true,
            isUpdatable: false,
            relationKind: "r"
        )

        XCTAssertFalse(descriptor.isEditable)
    }

    func testMakeColumnEditDescriptorRejectsByteaColumns() {
        let descriptor = PostgresRepository.makeColumnEditDescriptor(
            columnName: "payload",
            typeName: "bytea",
            isNullable: true,
            hasDefaultValue: false,
            isGenerated: false,
            isUpdatable: true,
            relationKind: "r"
        )

        XCTAssertFalse(descriptor.isEditable)
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
