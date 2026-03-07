import AppKit
import XCTest
@testable import ViewDB

@MainActor
final class DataGridViewCoordinatorTests: XCTestCase {
    func testIsValueVisuallyClippedWhenColumnIsTooNarrow() {
        XCTAssertTrue(
            DataGridView.Coordinator.isValueVisuallyClipped(
                "abcdefghijklmnopqrstuvwxyz",
                columnWidth: 84
            )
        )
    }

    func testIsValueVisuallyClippedForMultilineText() {
        XCTAssertTrue(
            DataGridView.Coordinator.isValueVisuallyClipped(
                "line1\nline2",
                columnWidth: 300
            )
        )
    }

    func testNextPopoverIntentTogglesSameCell() {
        let intent = DataGridView.Coordinator.nextPopoverIntent(
            currentCell: (row: 2, column: 1),
            isPopoverShown: true,
            clickedCell: (row: 2, column: 1),
            shouldOpen: true
        )

        XCTAssertEqual(intent, .close)
    }

    func testNextPopoverIntentSwitchesToDifferentCell() {
        let intent = DataGridView.Coordinator.nextPopoverIntent(
            currentCell: (row: 2, column: 1),
            isPopoverShown: true,
            clickedCell: (row: 3, column: 1),
            shouldOpen: true
        )

        XCTAssertEqual(intent, .open(row: 3, column: 1))
    }

    func testFormatPopoverValuePrettyPrintsJSONColumns() {
        let formatted = DataGridView.Coordinator.formatPopoverValue(
            #"{"z":1,"a":{"k":"v"}}"#,
            columnTypeName: "jsonb"
        )

        XCTAssertTrue(formatted.contains("\n"))
        XCTAssertTrue(formatted.contains("\"a\""))
    }

    func testFormatPopoverValueLeavesInvalidJSONUntouched() {
        let source = "{bad json"
        let formatted = DataGridView.Coordinator.formatPopoverValue(
            source,
            columnTypeName: "json"
        )

        XCTAssertEqual(formatted, source)
    }

    func testPopoverHeightExpandsWithMoreLines() {
        let short = DataGridView.Coordinator.popoverHeight(for: "one")
        let longer = DataGridView.Coordinator.popoverHeight(for: "one\ntwo\nthree\nfour\nfive")

        XCTAssertGreaterThan(longer, short)
    }

    func testPopoverHeightIsClampedToBounds() {
        let tiny = DataGridView.Coordinator.popoverHeight(for: "")
        let huge = DataGridView.Coordinator.popoverHeight(
            for: String(repeating: "x\n", count: 1000)
        )

        XCTAssertEqual(tiny, 72)
        XCTAssertEqual(huge, 540)
    }

    func testResponsivePopoverPlacementPrefersBelowWhenThereIsMoreSpace() {
        let placement = DataGridView.Coordinator.responsivePopoverPlacement(
            preferredHeight: 320,
            availableAbove: 140,
            availableBelow: 420
        )

        XCTAssertEqual(placement.edge, .maxY)
        XCTAssertEqual(placement.height, 320)
    }

    func testResponsivePopoverPlacementPrefersAboveWhenThereIsMoreSpace() {
        let placement = DataGridView.Coordinator.responsivePopoverPlacement(
            preferredHeight: 320,
            availableAbove: 500,
            availableBelow: 120
        )

        XCTAssertEqual(placement.edge, .minY)
        XCTAssertEqual(placement.height, 320)
    }

    func testResponsivePopoverPlacementClampsHeightToUsableViewport() {
        let placement = DataGridView.Coordinator.responsivePopoverPlacement(
            preferredHeight: 420,
            availableAbove: 90,
            availableBelow: 120
        )

        XCTAssertEqual(placement.edge, .maxY)
        XCTAssertEqual(placement.height, 104)
    }

    func testEditorDraftRequiresDirtyValueToSave() {
        let draft = GridCellEditorDraft(
            originalValue: "demo",
            currentValue: "demo",
            descriptor: ColumnEditDescriptor(
                columnName: "name",
                typeName: "text",
                isEditable: true,
                isNullable: false,
                hasDefaultValue: false,
                isGenerated: false,
                editorKind: .textField
            )
        )

        XCTAssertFalse(draft.canSave)
    }

    func testEditorDraftRejectsInvalidBooleanValue() {
        let draft = GridCellEditorDraft(
            originalValue: "false",
            currentValue: "maybe",
            descriptor: ColumnEditDescriptor(
                columnName: "enabled",
                typeName: "bool",
                isEditable: true,
                isNullable: false,
                hasDefaultValue: false,
                isGenerated: false,
                editorKind: .boolean
            )
        )

        XCTAssertFalse(draft.isValid)
        XCTAssertFalse(draft.canSave)
    }

    func testFormatEditorValueNormalizesBooleanAliases() {
        let descriptor = ColumnEditDescriptor(
            columnName: "enabled",
            typeName: "bool",
            isEditable: true,
            isNullable: false,
            hasDefaultValue: false,
            isGenerated: false,
            editorKind: .boolean
        )

        XCTAssertEqual(DataGridView.Coordinator.formatEditorValue("t", descriptor: descriptor), "true")
        XCTAssertEqual(DataGridView.Coordinator.unformatEditorValue("0", descriptor: descriptor), "false")
    }

    func testCoordinatorSkipsReloadWhenPayloadIsUnchanged() {
        let coordinator = DataGridView.Coordinator()
        let tableView = ReloadTrackingTableView()
        tableView.delegate = coordinator
        tableView.dataSource = coordinator
        coordinator.attach(tableView: tableView)

        let rows = [
            TableRowItem(
                id: 0,
                identity: .offset(0),
                values: [TableCellValue(previewText: "1", isTruncated: false)]
            ),
        ]

        coordinator.update(
            columns: ["id"],
            rows: rows,
            activeSort: nil,
            sortableColumns: [],
            onToggleSort: nil,
            onRequestFullValue: nil
        )
        XCTAssertEqual(tableView.reloadDataCallCount, 1)

        coordinator.update(
            columns: ["id"],
            rows: rows,
            activeSort: nil,
            sortableColumns: [],
            onToggleSort: nil,
            onRequestFullValue: nil
        )
        XCTAssertEqual(tableView.reloadDataCallCount, 1)
    }

    func testCoordinatorReloadsWhenRowsChange() {
        let coordinator = DataGridView.Coordinator()
        let tableView = ReloadTrackingTableView()
        tableView.delegate = coordinator
        tableView.dataSource = coordinator
        coordinator.attach(tableView: tableView)

        let initialRows = [
            TableRowItem(
                id: 0,
                identity: .offset(0),
                values: [TableCellValue(previewText: "1", isTruncated: false)]
            ),
        ]
        let updatedRows = [
            TableRowItem(
                id: 0,
                identity: .offset(0),
                values: [TableCellValue(previewText: "2", isTruncated: false)]
            ),
        ]

        coordinator.update(
            columns: ["id"],
            rows: initialRows,
            activeSort: nil,
            sortableColumns: [],
            onToggleSort: nil,
            onRequestFullValue: nil
        )
        XCTAssertEqual(tableView.reloadDataCallCount, 1)

        coordinator.update(
            columns: ["id"],
            rows: updatedRows,
            activeSort: nil,
            sortableColumns: [],
            onToggleSort: nil,
            onRequestFullValue: nil
        )
        XCTAssertEqual(tableView.reloadDataCallCount, 2)
    }
}

@MainActor
private final class ReloadTrackingTableView: GridTableView {
    private(set) var reloadDataCallCount = 0

    override func reloadData() {
        reloadDataCallCount += 1
        super.reloadData()
    }
}
