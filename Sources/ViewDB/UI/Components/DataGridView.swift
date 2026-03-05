import AppKit
import OSLog
import SwiftUI

struct DataGridView: NSViewRepresentable {
    typealias FullValueProvider = (_ rowIdentity: RowIdentity, _ columnName: String) async -> String?

    let columns: [String]
    let rows: [TableRowItem]
    let onRequestFullValue: FullValueProvider?

    init(
        columns: [String],
        rows: [TableRowItem],
        onRequestFullValue: FullValueProvider? = nil
    ) {
        self.columns = columns
        self.rows = rows
        self.onRequestFullValue = onRequestFullValue
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView()
        scrollView.drawsBackground = false
        scrollView.borderType = .noBorder
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true

        let tableView = GridTableView()
        tableView.usesAlternatingRowBackgroundColors = true
        tableView.rowHeight = 24
        tableView.intercellSpacing = NSSize(width: 0, height: 0)
        tableView.allowsColumnResizing = true
        tableView.allowsMultipleSelection = false
        tableView.allowsEmptySelection = true
        tableView.allowsTypeSelect = false
        tableView.headerView = NSTableHeaderView()
        tableView.columnAutoresizingStyle = .noColumnAutoresizing
        tableView.focusRingType = .none
        tableView.gridStyleMask = []

        tableView.delegate = context.coordinator
        tableView.dataSource = context.coordinator
        tableView.onCopyCommand = { [weak coordinator = context.coordinator] in
            coordinator?.copyFocusedCell()
        }
        tableView.onCellInteraction = { [weak coordinator = context.coordinator] row, column in
            coordinator?.focusCell(row: row, column: column)
        }

        scrollView.documentView = tableView
        context.coordinator.attach(tableView: tableView)
        context.coordinator.update(columns: columns, rows: rows, onRequestFullValue: onRequestFullValue)
        return scrollView
    }

    func updateNSView(_ nsView: NSScrollView, context: Context) {
        context.coordinator.update(columns: columns, rows: rows, onRequestFullValue: onRequestFullValue)
    }
}

class GridTableView: NSTableView {
    var onCopyCommand: (() -> Void)?
    var onCellInteraction: ((Int, Int) -> Void)?

    override func performKeyEquivalent(with event: NSEvent) -> Bool {
        if event.modifierFlags.contains(.command),
           event.charactersIgnoringModifiers?.lowercased() == "c" {
            onCopyCommand?()
            return true
        }

        return super.performKeyEquivalent(with: event)
    }

    override func mouseDown(with event: NSEvent) {
        updateFocusedCell(from: event)
        super.mouseDown(with: event)
    }

    override func rightMouseDown(with event: NSEvent) {
        updateFocusedCell(from: event)
        super.rightMouseDown(with: event)
    }

    private func updateFocusedCell(from event: NSEvent) {
        let point = convert(event.locationInWindow, from: nil)
        let row = self.row(at: point)
        let column = self.column(at: point)
        if row >= 0, column >= 0 {
            onCellInteraction?(row, column)
        }
    }
}

extension DataGridView {
    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        private static let defaultColumnWidth: CGFloat = 170
        private static let minColumnWidth: CGFloat = 84
        private static let cellIdentifier = NSUserInterfaceItemIdentifier("DataGridCell")
        private let logger = Logger(subsystem: "com.viewdb.ui", category: "grid")

        private weak var tableView: GridTableView?
        private var columns: [String] = []
        private var rows: [TableRowItem] = []
        private var onRequestFullValue: FullValueProvider?
        private var focusedCell: (row: Int, column: Int)?

        func attach(tableView: GridTableView) {
            self.tableView = tableView
            tableView.menu = makeContextMenu()
        }

        func update(
            columns: [String],
            rows: [TableRowItem],
            onRequestFullValue: FullValueProvider?
        ) {
            let start = CFAbsoluteTimeGetCurrent()
            self.onRequestFullValue = onRequestFullValue
            let columnsChanged = self.columns != columns
            let rowsChanged = self.rows != rows
            var updateReason = "none"

            if columnsChanged {
                self.columns = columns
                self.rows = rows
                rebuildColumns()
                updateReason = "columns"
            } else if rowsChanged {
                self.rows = rows
                tableView?.reloadData()
                updateReason = "rows"
            }

            if let focusedCell {
                if self.rows.indices.contains(focusedCell.row), self.columns.indices.contains(focusedCell.column) {
                    logUpdateTelemetry(
                        reason: updateReason,
                        elapsedMS: Int((CFAbsoluteTimeGetCurrent() - start) * 1000)
                    )
                    return
                }
                self.focusedCell = nil
            }
            logUpdateTelemetry(
                reason: updateReason,
                elapsedMS: Int((CFAbsoluteTimeGetCurrent() - start) * 1000)
            )
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            rows.count
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard let tableColumn,
                  let columnIndex = columnIndex(for: tableColumn) else {
                return nil
            }

            let cellValue = rows[safe: row]?.values[safe: columnIndex]?.previewText ?? ""

            if let existing = tableView.makeView(withIdentifier: Self.cellIdentifier, owner: nil) as? NSTableCellView,
               let textField = existing.textField {
                textField.stringValue = cellValue
                return existing
            }

            let cellView = NSTableCellView()
            cellView.identifier = Self.cellIdentifier

            let textField = NSTextField(labelWithString: cellValue)
            textField.font = .monospacedSystemFont(ofSize: 13, weight: .regular)
            textField.lineBreakMode = .byTruncatingTail
            textField.maximumNumberOfLines = 1
            textField.translatesAutoresizingMaskIntoConstraints = false
            textField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)

            cellView.textField = textField
            cellView.addSubview(textField)
            NSLayoutConstraint.activate([
                textField.leadingAnchor.constraint(equalTo: cellView.leadingAnchor, constant: 8),
                textField.trailingAnchor.constraint(equalTo: cellView.trailingAnchor, constant: -8),
                textField.centerYAnchor.constraint(equalTo: cellView.centerYAnchor),
            ])

            return cellView
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            guard let tableView, tableView.selectedRow >= 0 else {
                focusedCell = nil
                return
            }

            let column = focusedCell?.column ?? 0
            focusedCell = (row: tableView.selectedRow, column: max(0, min(column, max(0, columns.count - 1))))
        }

        func focusCell(row: Int, column: Int) {
            focusedCell = (row, column)
            tableView?.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        }

        @objc
        func copyCellMenuAction() {
            copyFocusedCell()
        }

        func copyFocusedCell() {
            guard let tableView else { return }
            guard let target = targetCell(from: tableView) else { return }

            Task { [weak self] in
                guard let self else { return }
                let text = await self.resolveCopyText(for: target)
                await MainActor.run {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(text, forType: .string)
                }
            }
        }

        private func resolveCopyText(for cell: (row: Int, column: Int)) async -> String {
            guard let row = rows[safe: cell.row],
                  let value = row.values[safe: cell.column],
                  let columnName = columns[safe: cell.column] else {
                return ""
            }

            if value.isTruncated,
               let onRequestFullValue,
               let fullValue = await onRequestFullValue(row.identity, columnName) {
                return fullValue
            }

            return value.previewText
        }

        private func targetCell(from tableView: GridTableView) -> (row: Int, column: Int)? {
            if tableView.clickedRow >= 0, tableView.clickedColumn >= 0 {
                return (tableView.clickedRow, tableView.clickedColumn)
            }

            if let focusedCell,
               rows.indices.contains(focusedCell.row),
               columns.indices.contains(focusedCell.column) {
                return focusedCell
            }

            if tableView.selectedRow >= 0 {
                let column = min(max(0, focusedCell?.column ?? 0), max(0, columns.count - 1))
                return (tableView.selectedRow, column)
            }

            return nil
        }

        private func rebuildColumns() {
            guard let tableView else { return }

            for column in tableView.tableColumns {
                tableView.removeTableColumn(column)
            }

            for (index, title) in columns.enumerated() {
                let tableColumn = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("col-\(index)"))
                tableColumn.title = title
                tableColumn.width = Self.defaultColumnWidth
                tableColumn.minWidth = Self.minColumnWidth
                tableColumn.resizingMask = [.autoresizingMask, .userResizingMask]
                tableView.addTableColumn(tableColumn)
            }

            tableView.reloadData()
        }

        private func columnIndex(for tableColumn: NSTableColumn) -> Int? {
            let rawValue = tableColumn.identifier.rawValue
            guard rawValue.hasPrefix("col-") else { return nil }
            return Int(rawValue.dropFirst(4))
        }

        private func makeContextMenu() -> NSMenu {
            let menu = NSMenu(title: "Data")
            let copyItem = NSMenuItem(
                title: "Copy Cell",
                action: #selector(copyCellMenuAction),
                keyEquivalent: ""
            )
            copyItem.target = self
            menu.addItem(copyItem)
            return menu
        }

        private func logUpdateTelemetry(reason: String, elapsedMS: Int) {
            guard reason != "none" else { return }
            logger.debug(
                "grid update reason=\(reason, privacy: .public) rows=\(self.rows.count, privacy: .public) cols=\(self.columns.count, privacy: .public) ms=\(elapsedMS, privacy: .public)"
            )
        }
    }
}
