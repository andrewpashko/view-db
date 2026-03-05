import AppKit
import XCTest
@testable import ViewDB

@MainActor
final class DataGridViewCoordinatorTests: XCTestCase {
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

        coordinator.update(columns: ["id"], rows: rows, onRequestFullValue: nil)
        XCTAssertEqual(tableView.reloadDataCallCount, 1)

        coordinator.update(columns: ["id"], rows: rows, onRequestFullValue: nil)
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

        coordinator.update(columns: ["id"], rows: initialRows, onRequestFullValue: nil)
        XCTAssertEqual(tableView.reloadDataCallCount, 1)

        coordinator.update(columns: ["id"], rows: updatedRows, onRequestFullValue: nil)
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
