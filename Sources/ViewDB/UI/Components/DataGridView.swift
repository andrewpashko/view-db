import AppKit
import OSLog
import SwiftUI

private enum DataGridMetrics {
    static let defaultColumnWidth: CGFloat = 170
    static let minColumnWidth: CGFloat = 84
    static let cellHorizontalPadding: CGFloat = 8
    static let cornerRadius: CGFloat = 10
    static let borderWidth: CGFloat = 1
    static let popoverMinWidth: CGFloat = 320
    static let popoverMaxWidth: CGFloat = 760
    static let popoverMinHeight: CGFloat = 72
    static let popoverMaxHeight: CGFloat = 540
}

struct DataGridView: NSViewRepresentable {
    typealias FullValueProvider = (_ rowIdentity: RowIdentity, _ columnName: String) async -> String?

    let columns: [String]
    let columnTypeNames: [String]
    let rows: [TableRowItem]
    let onRequestFullValue: FullValueProvider?

    init(
        columns: [String],
        columnTypeNames: [String] = [],
        rows: [TableRowItem],
        onRequestFullValue: FullValueProvider? = nil
    ) {
        self.columns = columns
        self.columnTypeNames = columnTypeNames
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
        style(scrollView)

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
        tableView.onPrimaryCellClick = { [weak coordinator = context.coordinator] row, column in
            coordinator?.handlePrimaryCellClick(row: row, column: column)
        }

        scrollView.documentView = tableView
        context.coordinator.attach(tableView: tableView)
        context.coordinator.update(
            columns: columns,
            rows: rows,
            columnTypeNames: columnTypeNames,
            onRequestFullValue: onRequestFullValue
        )
        return scrollView
    }

    func updateNSView(_ nsView: NSScrollView, context: Context) {
        style(nsView)
        context.coordinator.update(
            columns: columns,
            rows: rows,
            columnTypeNames: columnTypeNames,
            onRequestFullValue: onRequestFullValue
        )
    }

    private func style(_ scrollView: NSScrollView) {
        if !scrollView.wantsLayer {
            scrollView.wantsLayer = true
        }
        guard let layer = scrollView.layer else { return }

        layer.cornerRadius = DataGridMetrics.cornerRadius
        layer.masksToBounds = true
        layer.borderWidth = DataGridMetrics.borderWidth
        layer.borderColor = NSColor.separatorColor.withAlphaComponent(0.22).cgColor
    }
}

class GridTableView: NSTableView {
    var onCopyCommand: (() -> Void)?
    var onCellInteraction: ((Int, Int) -> Void)?
    var onPrimaryCellClick: ((Int, Int) -> Void)?

    override func performKeyEquivalent(with event: NSEvent) -> Bool {
        if event.modifierFlags.contains(.command),
           event.charactersIgnoringModifiers?.lowercased() == "c" {
            onCopyCommand?()
            return true
        }

        return super.performKeyEquivalent(with: event)
    }

    override func mouseDown(with event: NSEvent) {
        if let cell = updateFocusedCell(from: event) {
            onPrimaryCellClick?(cell.row, cell.column)
        }
        super.mouseDown(with: event)
    }

    override func rightMouseDown(with event: NSEvent) {
        _ = updateFocusedCell(from: event)
        super.rightMouseDown(with: event)
    }

    @discardableResult
    private func updateFocusedCell(from event: NSEvent) -> (row: Int, column: Int)? {
        let point = convert(event.locationInWindow, from: nil)
        let row = self.row(at: point)
        let column = self.column(at: point)
        if row >= 0, column >= 0 {
            onCellInteraction?(row, column)
            return (row, column)
        }
        return nil
    }
}

private final class GridCellPopoverViewController: NSViewController {
    private let scrollView = NSScrollView()
    private let textView = NSTextView()

    override func loadView() {
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true

        textView.isEditable = false
        textView.isSelectable = true
        textView.isRichText = false
        textView.importsGraphics = false
        textView.drawsBackground = false
        textView.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        textView.textContainerInset = NSSize(width: 8, height: 8)
        textView.isHorizontallyResizable = true
        textView.isVerticallyResizable = true
        textView.minSize = NSSize(width: 0, height: 0)
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.textContainer?.containerSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.textContainer?.widthTracksTextView = false
        textView.textContainer?.lineBreakMode = .byWordWrapping

        scrollView.documentView = textView
        view = scrollView
    }

    func setText(_ value: String) {
        textView.string = value
        textView.scrollToBeginningOfDocument(nil)
    }
}

extension DataGridView {
    @MainActor
    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate, NSPopoverDelegate {
        enum PopoverIntent: Equatable {
            case open(row: Int, column: Int)
            case close
        }

        private static let cellIdentifier = NSUserInterfaceItemIdentifier("DataGridCell")
        private static let cellFont = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
        private let logger = Logger(subsystem: "com.viewdb.ui", category: "grid")

        private weak var tableView: GridTableView?
        private var columns: [String] = []
        private var rows: [TableRowItem] = []
        private var columnTypeNames: [String] = []
        private var columnIndexByIdentifier: [NSUserInterfaceItemIdentifier: Int] = [:]
        private var onRequestFullValue: FullValueProvider?
        private var focusedCell: (row: Int, column: Int)?

        private var popover: NSPopover?
        private let popoverController = GridCellPopoverViewController()
        private var popoverCell: (row: Int, column: Int)?
        private var popoverLoadTask: Task<Void, Never>?
        private var popoverRequestID: UUID?

        func attach(tableView: GridTableView) {
            self.tableView = tableView
            tableView.menu = makeContextMenu()
        }

        func update(
            columns: [String],
            rows: [TableRowItem],
            columnTypeNames: [String] = [],
            onRequestFullValue: FullValueProvider?
        ) {
            let start = CFAbsoluteTimeGetCurrent()
            self.onRequestFullValue = onRequestFullValue
            let columnsChanged = self.columns != columns
            let rowsChanged = self.rows != rows
            let typesChanged = self.columnTypeNames != columnTypeNames
            var updateReason = "none"

            if columnsChanged {
                self.columns = columns
                self.rows = rows
                self.columnTypeNames = columnTypeNames
                rebuildColumns()
                updateReason = "columns"
            } else if rowsChanged {
                self.rows = rows
                self.columnTypeNames = columnTypeNames
                tableView?.reloadData()
                updateReason = "rows"
            } else if typesChanged {
                self.columnTypeNames = columnTypeNames
                updateReason = "types"
            }

            if columnsChanged || rowsChanged || typesChanged {
                closePopover()
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
            textField.font = Self.cellFont
            textField.lineBreakMode = .byTruncatingTail
            textField.maximumNumberOfLines = 1
            textField.translatesAutoresizingMaskIntoConstraints = false
            textField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)

            cellView.textField = textField
            cellView.addSubview(textField)
            NSLayoutConstraint.activate([
                textField.leadingAnchor.constraint(equalTo: cellView.leadingAnchor, constant: DataGridMetrics.cellHorizontalPadding),
                textField.trailingAnchor.constraint(equalTo: cellView.trailingAnchor, constant: -DataGridMetrics.cellHorizontalPadding),
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

        func handlePrimaryCellClick(row: Int, column: Int) {
            guard let rowItem = rows[safe: row],
                  let value = rowItem.values[safe: column],
                  let columnName = columns[safe: column] else {
                closePopover()
                return
            }

            let visuallyClipped = isCellVisuallyClipped(row: row, column: column, text: value.previewText)
            let shouldOpen = value.isTruncated || visuallyClipped
            let clickedCell = (row: row, column: column)

            switch Self.nextPopoverIntent(
                currentCell: popoverCell,
                isPopoverShown: popover?.isShown == true,
                clickedCell: clickedCell,
                shouldOpen: shouldOpen
            ) {
            case .close:
                closePopover()
                return
            case .open:
                break
            }

            let columnTypeName = columnTypeNames[safe: column]
            let initialText = Self.formatPopoverValue(value.previewText, columnTypeName: columnTypeName)
            showPopover(for: clickedCell, text: initialText)

            popoverLoadTask?.cancel()
            let requestID = UUID()
            popoverRequestID = requestID

            popoverLoadTask = Task { [weak self] in
                guard let self else { return }
                let resolvedText = await self.resolvePopoverText(
                    rowItem: rowItem,
                    value: value,
                    columnName: columnName,
                    columnIndex: column
                )
                await MainActor.run {
                    guard self.popoverRequestID == requestID,
                          let current = self.popoverCell,
                          current.row == clickedCell.row,
                          current.column == clickedCell.column,
                          self.popover?.isShown == true else {
                        return
                    }
                    self.updatePopoverContent(resolvedText, forColumn: clickedCell.column)
                }
            }
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

        func popoverDidClose(_ notification: Notification) {
            popoverLoadTask?.cancel()
            popoverLoadTask = nil
            popoverRequestID = nil
            popoverCell = nil
        }

        private func resolvePopoverText(
            rowItem: TableRowItem,
            value: TableCellValue,
            columnName: String,
            columnIndex: Int
        ) async -> String {
            var resolved = value.previewText
            if value.isTruncated,
               let onRequestFullValue,
               let fullValue = await onRequestFullValue(rowItem.identity, columnName) {
                resolved = fullValue
            }

            return Self.formatPopoverValue(
                resolved,
                columnTypeName: columnTypeNames[safe: columnIndex]
            )
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
            columnIndexByIdentifier.removeAll(keepingCapacity: true)

            for (index, title) in columns.enumerated() {
                let identifier = NSUserInterfaceItemIdentifier("col-\(index)")
                let tableColumn = NSTableColumn(identifier: identifier)
                tableColumn.title = title
                tableColumn.width = DataGridMetrics.defaultColumnWidth
                tableColumn.minWidth = DataGridMetrics.minColumnWidth
                tableColumn.resizingMask = [.autoresizingMask, .userResizingMask]
                tableColumn.headerCell = PaddedTableHeaderCell(textCell: title)
                tableView.addTableColumn(tableColumn)
                columnIndexByIdentifier[identifier] = index
            }

            tableView.reloadData()
        }

        private func columnIndex(for tableColumn: NSTableColumn) -> Int? {
            columnIndexByIdentifier[tableColumn.identifier]
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

        private func showPopover(for cell: (row: Int, column: Int), text: String) {
            guard let tableView else { return }
            guard rows.indices.contains(cell.row), columns.indices.contains(cell.column) else { return }

            let popover = makePopoverIfNeeded()
            updatePopoverContent(text, forColumn: cell.column)

            if popover.isShown {
                popover.performClose(nil)
            }

            popover.show(
                relativeTo: tableView.frameOfCell(atColumn: cell.column, row: cell.row),
                of: tableView,
                preferredEdge: .maxY
            )
            popoverCell = cell
        }

        private func updatePopoverContent(_ text: String, forColumn column: Int) {
            let width = popoverWidth(for: column)
            let height = Self.popoverHeight(for: text)
            popover?.contentSize = NSSize(width: width, height: height)
            popoverController.setText(text)
        }

        private func closePopover() {
            popoverLoadTask?.cancel()
            popoverLoadTask = nil
            popoverRequestID = nil
            popoverCell = nil
            popover?.performClose(nil)
        }

        private func makePopoverIfNeeded() -> NSPopover {
            if let popover {
                return popover
            }

            let popover = NSPopover()
            popover.behavior = .transient
            popover.animates = true
            popover.contentViewController = popoverController
            popover.delegate = self
            self.popover = popover
            return popover
        }

        private func popoverWidth(for column: Int) -> CGFloat {
            let baseWidth = tableView?.tableColumns[safe: column].map(\.width) ?? DataGridMetrics.defaultColumnWidth
            let proposed = baseWidth * 2.2
            return min(max(DataGridMetrics.popoverMinWidth, proposed), DataGridMetrics.popoverMaxWidth)
        }

        private func isCellVisuallyClipped(row: Int, column: Int, text: String) -> Bool {
            guard rows.indices.contains(row),
                  columns.indices.contains(column) else {
                return false
            }

            let width = tableView?.tableColumns[safe: column].map(\.width) ?? DataGridMetrics.defaultColumnWidth
            return Self.isValueVisuallyClipped(text, columnWidth: width)
        }

        private func logUpdateTelemetry(reason: String, elapsedMS: Int) {
            guard reason != "none" else { return }
            logger.debug(
                "grid update reason=\(reason, privacy: .public) rows=\(self.rows.count, privacy: .public) cols=\(self.columns.count, privacy: .public) ms=\(elapsedMS, privacy: .public)"
            )
        }

        static func nextPopoverIntent(
            currentCell: (row: Int, column: Int)?,
            isPopoverShown: Bool,
            clickedCell: (row: Int, column: Int),
            shouldOpen: Bool
        ) -> PopoverIntent {
            guard shouldOpen else { return .close }

            if isPopoverShown,
               let currentCell,
               currentCell.row == clickedCell.row,
               currentCell.column == clickedCell.column {
                return .close
            }

            return .open(row: clickedCell.row, column: clickedCell.column)
        }

        static func isValueVisuallyClipped(
            _ value: String,
            columnWidth: CGFloat,
            font: NSFont = Coordinator.cellFont,
            horizontalPadding: CGFloat = DataGridMetrics.cellHorizontalPadding
        ) -> Bool {
            guard !value.isEmpty else { return false }
            if value.contains(where: \.isNewline) {
                return true
            }

            let availableWidth = columnWidth - (horizontalPadding * 2)
            guard availableWidth > 0 else {
                return true
            }

            let measuredWidth = (value as NSString).size(withAttributes: [.font: font]).width
            return measuredWidth > availableWidth
        }

        static func formatPopoverValue(_ value: String, columnTypeName: String?) -> String {
            guard let type = columnTypeName?.lowercased(),
                  type == "json" || type == "jsonb" else {
                return value
            }

            guard let data = value.data(using: .utf8),
                  let object = try? JSONSerialization.jsonObject(with: data, options: []),
                  let prettyData = try? JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys]),
                  let prettyValue = String(data: prettyData, encoding: .utf8) else {
                return value
            }

            return prettyValue
        }

        static func popoverHeight(
            for value: String,
            font: NSFont = Coordinator.cellFont,
            minHeight: CGFloat = DataGridMetrics.popoverMinHeight,
            maxHeight: CGFloat = DataGridMetrics.popoverMaxHeight
        ) -> CGFloat {
            let lineCount = max(1, value.split(separator: "\n", omittingEmptySubsequences: false).count)
            let lineHeight = ceil(font.ascender - font.descender + font.leading)
            let textHeight = CGFloat(lineCount) * lineHeight
            let paddedHeight = textHeight + 24
            return min(max(minHeight, paddedHeight), maxHeight)
        }
    }
}

private final class PaddedTableHeaderCell: NSTableHeaderCell {
    override init(textCell string: String) {
        super.init(textCell: string)
        font = .monospacedSystemFont(ofSize: 13, weight: .semibold)
        alignment = .left
        lineBreakMode = .byTruncatingTail
    }

    required init(coder: NSCoder) {
        super.init(coder: coder)
    }

    override func drawingRect(forBounds rect: NSRect) -> NSRect {
        rect.insetBy(dx: DataGridMetrics.cellHorizontalPadding, dy: 0)
    }
}
