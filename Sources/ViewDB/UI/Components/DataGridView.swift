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
    static let popoverWindowPadding: CGFloat = 16
    static let popoverArrowClearance: CGFloat = 16
}

struct DataGridView: NSViewRepresentable {
    typealias FullValueProvider = (_ rowIdentity: RowIdentity, _ columnName: String) async -> String?
    typealias BeginEditValueProvider = (_ row: TableRowItem, _ columnName: String) async -> String?
    enum CommitEditOutcome: Equatable {
        case success(String)
        case failure(String)
    }

    typealias CommitEditProvider = (_ row: TableRowItem, _ columnName: String, _ value: String?) async -> CommitEditOutcome

    let columns: [String]
    let columnTypeNames: [String]
    let columnEditDescriptors: [ColumnEditDescriptor]
    let rows: [TableRowItem]
    let activeSort: TableSort?
    let sortableColumns: Set<String>
    let onToggleSort: ((String) -> Void)?
    let onRequestFullValue: FullValueProvider?
    let onBeginEdit: BeginEditValueProvider?
    let onCommitEdit: CommitEditProvider?

    init(
        columns: [String],
        columnTypeNames: [String] = [],
        columnEditDescriptors: [ColumnEditDescriptor] = [],
        rows: [TableRowItem],
        activeSort: TableSort? = nil,
        sortableColumns: Set<String> = [],
        onToggleSort: ((String) -> Void)? = nil,
        onRequestFullValue: FullValueProvider? = nil,
        onBeginEdit: BeginEditValueProvider? = nil,
        onCommitEdit: CommitEditProvider? = nil
    ) {
        self.columns = columns
        self.columnTypeNames = columnTypeNames
        self.columnEditDescriptors = columnEditDescriptors
        self.rows = rows
        self.activeSort = activeSort
        self.sortableColumns = sortableColumns
        self.onToggleSort = onToggleSort
        self.onRequestFullValue = onRequestFullValue
        self.onBeginEdit = onBeginEdit
        self.onCommitEdit = onCommitEdit
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
            columnEditDescriptors: columnEditDescriptors,
            activeSort: activeSort,
            sortableColumns: sortableColumns,
            onToggleSort: onToggleSort,
            onRequestFullValue: onRequestFullValue,
            onBeginEdit: onBeginEdit,
            onCommitEdit: onCommitEdit
        )
        return scrollView
    }

    func updateNSView(_ nsView: NSScrollView, context: Context) {
        style(nsView)
        context.coordinator.update(
            columns: columns,
            rows: rows,
            columnTypeNames: columnTypeNames,
            columnEditDescriptors: columnEditDescriptors,
            activeSort: activeSort,
            sortableColumns: sortableColumns,
            onToggleSort: onToggleSort,
            onRequestFullValue: onRequestFullValue,
            onBeginEdit: onBeginEdit,
            onCommitEdit: onCommitEdit
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
    var onPrimaryCellDoubleClick: ((Int, Int) -> Void)?
    var onBeginEditCommand: (() -> Void)?
    var onEscapeCommand: (() -> Void)?

    override var acceptsFirstResponder: Bool {
        false
    }

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
            if event.clickCount >= 2 {
                onPrimaryCellDoubleClick?(cell.row, cell.column)
            } else {
                onPrimaryCellClick?(cell.row, cell.column)
            }
        }
        super.mouseDown(with: event)
    }

    override func keyDown(with event: NSEvent) {
        switch Int(event.keyCode) {
        case 36, 76:
            onBeginEditCommand?()
            return
        case 53:
            onEscapeCommand?()
            return
        default:
            super.keyDown(with: event)
        }
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

struct GridCellEditorDraft: Equatable {
    let originalValue: String?
    let descriptor: ColumnEditDescriptor
    var value: String
    var isNull: Bool
    var errorMessage: String?
    var isSaving: Bool

    init(
        originalValue: String?,
        currentValue: String?,
        descriptor: ColumnEditDescriptor,
        errorMessage: String? = nil,
        isSaving: Bool = false
    ) {
        let resolvedValue: String
        if let currentValue {
            resolvedValue = currentValue
        } else {
            switch descriptor.editorKind {
            case .boolean:
                resolvedValue = "false"
            case .enumeration(let options):
                resolvedValue = options.first ?? ""
            case .textField, .textArea:
                resolvedValue = ""
            }
        }

        self.originalValue = originalValue
        self.descriptor = descriptor
        self.value = resolvedValue
        self.isNull = currentValue == nil
        self.errorMessage = errorMessage
        self.isSaving = isSaving
    }

    var normalizedValue: String? {
        if isNull {
            return nil
        }
        return value
    }

    var isDirty: Bool {
        normalizedValue != originalValue
    }

    var isValid: Bool {
        if isNull {
            return descriptor.isNullable
        }

        switch descriptor.editorKind {
        case .boolean:
            return ["true", "false"].contains(value.lowercased())
        case .enumeration(let options):
            return options.contains(value)
        case .textField, .textArea:
            return true
        }
    }

    var canSave: Bool {
        isDirty && isValid && !isSaving
    }
}

private struct GridCellEditorView: View {
    let title: String
    let descriptor: ColumnEditDescriptor
    let renderID: String
    let initialOriginalValue: String?
    let initialCurrentValue: String?
    let initialErrorMessage: String?
    let initialIsSaving: Bool
    let onSave: (String?) -> Void
    let onCancel: () -> Void

    @State private var draft: GridCellEditorDraft

    init(
        title: String,
        descriptor: ColumnEditDescriptor,
        renderID: String,
        originalValue: String?,
        currentValue: String?,
        errorMessage: String? = nil,
        isSaving: Bool = false,
        onSave: @escaping (String?) -> Void,
        onCancel: @escaping () -> Void
    ) {
        self.title = title
        self.descriptor = descriptor
        self.renderID = renderID
        self.initialOriginalValue = originalValue
        self.initialCurrentValue = currentValue
        self.initialErrorMessage = errorMessage
        self.initialIsSaving = isSaving
        self.onSave = onSave
        self.onCancel = onCancel
        _draft = State(initialValue: GridCellEditorDraft(
            originalValue: originalValue,
            currentValue: currentValue,
            descriptor: descriptor,
            errorMessage: errorMessage,
            isSaving: isSaving
        ))
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(title)
                    .font(.system(.headline, design: .monospaced))
                Spacer(minLength: 0)
                Text(descriptor.typeName)
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
            }

            if descriptor.isNullable {
                Toggle("NULL", isOn: $draft.isNull)
                    .toggleStyle(.checkbox)
            }

            editor
                .disabled(draft.isNull || draft.isSaving)

            if let errorMessage = draft.errorMessage, !errorMessage.isEmpty {
                Text(errorMessage)
                    .font(.caption)
                    .foregroundStyle(.orange)
            }

            HStack {
                Spacer(minLength: 0)
                Button("Cancel", action: onCancel)
                    .keyboardShortcut(.escape, modifiers: [])
                saveButton
            }
        }
        .padding(14)
        .frame(minWidth: DataGridMetrics.popoverMinWidth, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onChange(of: renderID) { _, _ in
            draft = GridCellEditorDraft(
                originalValue: initialOriginalValue,
                currentValue: initialCurrentValue,
                descriptor: descriptor,
                errorMessage: initialErrorMessage,
                isSaving: initialIsSaving
            )
        }
    }

    @ViewBuilder
    private var editor: some View {
        switch descriptor.editorKind {
        case .boolean:
            Picker("Value", selection: $draft.value) {
                Text("true").tag("true")
                Text("false").tag("false")
            }
            .labelsHidden()
            .pickerStyle(.segmented)
        case .enumeration(let options):
            Picker("Value", selection: $draft.value) {
                ForEach(options, id: \.self) { option in
                    Text(option).tag(option)
                }
            }
            .labelsHidden()
        case .textArea:
            TextEditor(text: $draft.value)
                .font(.system(.body, design: .monospaced))
                .scrollContentBackground(.hidden)
                .frame(minHeight: 120, idealHeight: 220, maxHeight: .infinity)
                .overlay {
                    RoundedRectangle(cornerRadius: 8)
                        .strokeBorder(Color.secondary.opacity(0.18), lineWidth: 1)
                }
        case .textField:
            TextField("Value", text: $draft.value)
                .textFieldStyle(.roundedBorder)
                .font(.system(.body, design: .monospaced))
        }
    }

    @ViewBuilder
    private var saveButton: some View {
        switch descriptor.editorKind {
        case .textArea:
            Button("Save") {
                onSave(draft.normalizedValue)
            }
            .keyboardShortcut(.return, modifiers: [.command])
            .disabled(!draft.canSave)
        case .boolean, .enumeration, .textField:
            Button("Save") {
                onSave(draft.normalizedValue)
            }
            .keyboardShortcut(.defaultAction)
            .disabled(!draft.canSave)
        }
    }
}

private final class GridCellEditorPopoverViewController: NSViewController {
    private var hostingController: NSHostingController<GridCellEditorView>?

    override func loadView() {
        view = NSView()
    }

    func render(
        title: String,
        descriptor: ColumnEditDescriptor,
        originalValue: String?,
        currentValue: String?,
        errorMessage: String?,
        isSaving: Bool,
        onSave: @escaping (String?) -> Void,
        onCancel: @escaping () -> Void
    ) {
        let renderID = [
            title,
            descriptor.typeName,
            String(describing: descriptor.editorKind),
            originalValue ?? "<nil>",
            currentValue ?? "<nil>",
            errorMessage ?? "",
            isSaving ? "saving" : "idle",
        ].joined(separator: "|")
        let rootView = GridCellEditorView(
            title: title,
            descriptor: descriptor,
            renderID: renderID,
            originalValue: originalValue,
            currentValue: currentValue,
            errorMessage: errorMessage,
            isSaving: isSaving,
            onSave: onSave,
            onCancel: onCancel
        )

        if let hostingController {
            hostingController.rootView = rootView
        } else {
            let controller = NSHostingController(rootView: rootView)
            addChild(controller)
            controller.view.translatesAutoresizingMaskIntoConstraints = false
            view.addSubview(controller.view)
            NSLayoutConstraint.activate([
                controller.view.leadingAnchor.constraint(equalTo: view.leadingAnchor),
                controller.view.trailingAnchor.constraint(equalTo: view.trailingAnchor),
                controller.view.topAnchor.constraint(equalTo: view.topAnchor),
                controller.view.bottomAnchor.constraint(equalTo: view.bottomAnchor),
            ])
            hostingController = controller
        }
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
        private var columnEditDescriptors: [ColumnEditDescriptor] = []
        private var activeSort: TableSort?
        private var sortableColumns: Set<String> = []
        private var columnIndexByIdentifier: [NSUserInterfaceItemIdentifier: Int] = [:]
        private var onToggleSort: ((String) -> Void)?
        private var onRequestFullValue: FullValueProvider?
        private var onBeginEdit: BeginEditValueProvider?
        private var onCommitEdit: CommitEditProvider?
        private var focusedCell: (row: Int, column: Int)?

        private var previewPopover: NSPopover?
        private let previewPopoverController = GridCellPopoverViewController()
        private var previewPopoverCell: (row: Int, column: Int)?
        private var previewLoadTask: Task<Void, Never>?
        private var previewRequestID: UUID?

        private var editorPopover: NSPopover?
        private let editorPopoverController = GridCellEditorPopoverViewController()
        private var editorPopoverCell: (row: Int, column: Int)?
        private var editorOriginalValue: String?
        private var editorLoadTask: Task<Void, Never>?
        private var editorSaveTask: Task<Void, Never>?

        func attach(tableView: GridTableView) {
            self.tableView = tableView
            tableView.menu = makeContextMenu()
            tableView.onPrimaryCellDoubleClick = { [weak self] row, column in
                self?.handleEditRequest(row: row, column: column)
            }
            tableView.onBeginEditCommand = { [weak self] in
                self?.handleBeginEditCommand()
            }
            tableView.onEscapeCommand = { [weak self] in
                self?.handleEscapeCommand()
            }
        }

        func update(
            columns: [String],
            rows: [TableRowItem],
            columnTypeNames: [String] = [],
            columnEditDescriptors: [ColumnEditDescriptor] = [],
            activeSort: TableSort?,
            sortableColumns: Set<String>,
            onToggleSort: ((String) -> Void)?,
            onRequestFullValue: FullValueProvider?,
            onBeginEdit: BeginEditValueProvider? = nil,
            onCommitEdit: CommitEditProvider? = nil
        ) {
            let start = CFAbsoluteTimeGetCurrent()
            self.onRequestFullValue = onRequestFullValue
            self.onToggleSort = onToggleSort
            self.onBeginEdit = onBeginEdit
            self.onCommitEdit = onCommitEdit
            let columnsChanged = self.columns != columns
            let rowsChanged = self.rows != rows
            let typesChanged = self.columnTypeNames != columnTypeNames
            let editDescriptorsChanged = self.columnEditDescriptors != columnEditDescriptors
            let sortChanged = self.activeSort != activeSort
            let sortableChanged = self.sortableColumns != sortableColumns
            var updateReason = "none"

            if columnsChanged {
                self.columns = columns
                self.rows = rows
                self.columnTypeNames = columnTypeNames
                self.columnEditDescriptors = columnEditDescriptors
                self.activeSort = activeSort
                self.sortableColumns = sortableColumns
                rebuildColumns()
                updateReason = "columns"
            } else if rowsChanged {
                self.rows = rows
                self.columnTypeNames = columnTypeNames
                self.columnEditDescriptors = columnEditDescriptors
                self.activeSort = activeSort
                self.sortableColumns = sortableColumns
                tableView?.reloadData()
                updateReason = "rows"
            } else if typesChanged {
                self.columnTypeNames = columnTypeNames
                self.columnEditDescriptors = columnEditDescriptors
                self.activeSort = activeSort
                self.sortableColumns = sortableColumns
                updateReason = "types"
            } else if editDescriptorsChanged {
                self.columnEditDescriptors = columnEditDescriptors
                updateReason = "edit-meta"
            } else if sortChanged || sortableChanged {
                self.activeSort = activeSort
                self.sortableColumns = sortableColumns
                updateReason = sortChanged ? "sort" : "sortable"
            }

            if columnsChanged || rowsChanged || typesChanged || editDescriptorsChanged {
                closePreviewPopover()
                closeEditorPopover()
            }

            if columnsChanged || sortChanged || sortableChanged {
                updateSortIndicators()
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
            let displayText = Self.normalizeCellDisplayText(cellValue)

            if let existing = tableView.makeView(withIdentifier: Self.cellIdentifier, owner: nil) as? NSTableCellView,
               let textField = existing.textField {
                textField.stringValue = displayText
                return existing
            }

            let cellView = NSTableCellView()
            cellView.identifier = Self.cellIdentifier

            let textField = NSTextField(labelWithString: displayText)
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

        func tableView(_ tableView: NSTableView, didClick tableColumn: NSTableColumn) {
            guard let index = columnIndex(for: tableColumn),
                  let columnName = columns[safe: index],
                  sortableColumns.contains(columnName) else {
                return
            }
            onToggleSort?(columnName)
        }

        func focusCell(row: Int, column: Int) {
            focusedCell = (row, column)
            tableView?.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        }

        func handlePrimaryCellClick(row: Int, column: Int) {
            guard let rowItem = rows[safe: row],
                  let value = rowItem.values[safe: column],
                  let columnName = columns[safe: column] else {
                closePreviewPopover()
                return
            }

            let visuallyClipped = isCellVisuallyClipped(row: row, column: column, text: value.previewText)
            let shouldOpen = value.isTruncated || visuallyClipped
            let clickedCell = (row: row, column: column)

            switch Self.nextPopoverIntent(
                currentCell: previewPopoverCell,
                isPopoverShown: previewPopover?.isShown == true,
                clickedCell: clickedCell,
                shouldOpen: shouldOpen
            ) {
            case .close:
                closePreviewPopover()
                return
            case .open:
                break
            }

            let columnTypeName = columnTypeNames[safe: column]
            let initialText = Self.formatPopoverValue(value.previewText, columnTypeName: columnTypeName)
            closeEditorPopover()
            showPreviewPopover(for: clickedCell, text: initialText)

            previewLoadTask?.cancel()
            let requestID = UUID()
            previewRequestID = requestID

            previewLoadTask = Task { [weak self] in
                guard let self else { return }
                let resolvedText = await self.resolvePopoverText(
                    rowItem: rowItem,
                    value: value,
                    columnName: columnName,
                    columnIndex: column
                )
                await MainActor.run {
                    guard self.previewRequestID == requestID,
                          let current = self.previewPopoverCell,
                          current.row == clickedCell.row,
                          current.column == clickedCell.column,
                          self.previewPopover?.isShown == true else {
                        return
                    }
                    self.updatePreviewPopoverContent(resolvedText, forColumn: clickedCell.column)
                }
            }
        }

        func handleEditRequest(row: Int, column: Int) {
            guard let rowItem = rows[safe: row],
                  rowItem.editLocator != nil,
                  let columnName = columns[safe: column],
                  let descriptor = editDescriptor(for: column),
                  descriptor.isEditable else {
                return
            }

            closePreviewPopover()
            closeEditorPopover()
            editorLoadTask?.cancel()
            editorSaveTask?.cancel()

            let clickedCell = (row: row, column: column)
            let fallbackValue = rowItem.values[safe: column]?.previewText
            let fallbackText: String?
            if let fallbackValue, fallbackValue != "NULL" {
                fallbackText = Self.formatEditorValue(fallbackValue, descriptor: descriptor)
            } else {
                fallbackText = nil
            }
            editorOriginalValue = fallbackText

            showEditorPopover(
                for: clickedCell,
                descriptor: descriptor,
                originalValue: fallbackText,
                currentValue: fallbackText,
                errorMessage: nil,
                isSaving: false
            )

            editorLoadTask = Task { [weak self] in
                guard let self,
                      let onBeginEdit = self.onBeginEdit else { return }
                let fullValue = await onBeginEdit(rowItem, columnName)
                await MainActor.run {
                    guard let current = self.editorPopoverCell,
                          current.row == clickedCell.row,
                          current.column == clickedCell.column,
                          self.editorPopover?.isShown == true else {
                        return
                    }
                    self.showEditorPopover(
                        for: clickedCell,
                        descriptor: descriptor,
                        originalValue: fullValue.map { Self.formatEditorValue($0, descriptor: descriptor) },
                        currentValue: fullValue.map { Self.formatEditorValue($0, descriptor: descriptor) },
                        errorMessage: nil,
                        isSaving: false
                    )
                    self.editorOriginalValue = fullValue.map { Self.formatEditorValue($0, descriptor: descriptor) }
                }
            }
        }

        func handleBeginEditCommand() {
            guard let tableView,
                  let target = targetCell(from: tableView) else {
                return
            }
            handleEditRequest(row: target.row, column: target.column)
        }

        func handleEscapeCommand() {
            if editorPopover?.isShown == true {
                closeEditorPopover()
                return
            }

            if previewPopover?.isShown == true {
                closePreviewPopover()
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
            guard let popover = notification.object as? NSPopover else { return }

            if popover.contentViewController === previewPopoverController {
                previewLoadTask?.cancel()
                previewLoadTask = nil
                previewRequestID = nil
                previewPopoverCell = nil
                if popover === previewPopover {
                    previewPopover = nil
                }
            }

            if popover.contentViewController === editorPopoverController {
                editorLoadTask?.cancel()
                editorSaveTask?.cancel()
                editorLoadTask = nil
                editorSaveTask = nil
                editorPopoverCell = nil
                editorOriginalValue = nil
                if popover === editorPopover {
                    editorPopover = nil
                }
            }
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

        private func editDescriptor(for column: Int) -> ColumnEditDescriptor? {
            guard let columnName = columns[safe: column] else { return nil }
            return columnEditDescriptors.first(where: { $0.columnName == columnName })
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

            updateSortIndicators()
            tableView.reloadData()
            refreshHeaderRendering()
        }

        private func refreshHeaderRendering() {
            guard let headerView = tableView?.headerView else { return }
            headerView.needsDisplay = true
            DispatchQueue.main.async { [weak headerView] in
                headerView?.needsDisplay = true
                headerView?.displayIfNeeded()
            }
        }

        private func updateSortIndicators() {
            guard let tableView else { return }

            let ascending = NSImage(
                systemSymbolName: "arrowtriangle.up.fill",
                accessibilityDescription: "Sorted ascending"
            )
            let descending = NSImage(
                systemSymbolName: "arrowtriangle.down.fill",
                accessibilityDescription: "Sorted descending"
            )

            for tableColumn in tableView.tableColumns {
                guard let index = columnIndex(for: tableColumn),
                      let columnName = columns[safe: index] else {
                    continue
                }

                let image: NSImage?
                if let activeSort, activeSort.column == columnName {
                    image = activeSort.direction == .ascending ? ascending : descending
                } else {
                    image = nil
                }
                tableView.setIndicatorImage(image, in: tableColumn)

                if sortableColumns.contains(columnName) {
                    tableColumn.headerToolTip = "Click to sort"
                } else {
                    tableColumn.headerToolTip = nil
                }
            }
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

        private func showPreviewPopover(for cell: (row: Int, column: Int), text: String) {
            guard let tableView else { return }
            guard rows.indices.contains(cell.row), columns.indices.contains(cell.column) else { return }

            closePreviewPopover()
            let popover = makePreviewPopoverIfNeeded()
            updatePreviewPopoverContent(text, forColumn: cell.column)

            popover.show(
                relativeTo: tableView.frameOfCell(atColumn: cell.column, row: cell.row),
                of: tableView,
                preferredEdge: .maxY
            )
            previewPopoverCell = cell
        }

        private func updatePreviewPopoverContent(_ text: String, forColumn column: Int) {
            let width = popoverWidth(for: column)
            let height = Self.popoverHeight(for: text)
            if let previewPopover {
                applyPopoverSize(previewPopover, width: width, height: height)
            }
            previewPopoverController.setText(text)
        }

        private func showEditorPopover(
            for cell: (row: Int, column: Int),
            descriptor: ColumnEditDescriptor,
            originalValue: String?,
            currentValue: String?,
            errorMessage: String?,
            isSaving: Bool
        ) {
            guard let tableView,
                  rows.indices.contains(cell.row),
                  columns.indices.contains(cell.column),
                  let columnName = columns[safe: cell.column],
                  let rowItem = rows[safe: cell.row] else {
                return
            }

            let popover = makeEditorPopoverIfNeeded()
            editorPopoverController.render(
                title: columnName,
                descriptor: descriptor,
                originalValue: originalValue,
                currentValue: currentValue,
                errorMessage: errorMessage,
                isSaving: isSaving,
                onSave: { [weak self] newValue in
                    self?.commitEditorValue(for: cell, rowItem: rowItem, columnName: columnName, value: newValue)
                },
                onCancel: { [weak self] in
                    self?.closeEditorPopover()
                }
            )

            let width = popoverWidth(for: cell.column)
            let preferredHeight = Self.editorPopoverHeight(descriptor: descriptor, value: currentValue ?? "")
            let placement = editorPopoverPlacement(for: cell, preferredHeight: preferredHeight)
            applyPopoverSize(popover, width: width, height: placement.height)
            if !popover.isShown {
                popover.show(
                    relativeTo: tableView.frameOfCell(atColumn: cell.column, row: cell.row),
                    of: tableView,
                    preferredEdge: placement.edge
                )
            }
            editorPopoverCell = cell
        }

        private func applyPopoverSize(_ popover: NSPopover, width: CGFloat, height: CGFloat) {
            let size = NSSize(width: width, height: height)
            popover.contentSize = size
            popover.contentViewController?.preferredContentSize = size

            // Re-apply after presentation to avoid occasional stale cached size from prior openings.
            DispatchQueue.main.async { [weak popover] in
                guard let popover, popover.isShown else { return }
                popover.contentSize = size
                popover.contentViewController?.preferredContentSize = size
            }
        }

        private func commitEditorValue(
            for cell: (row: Int, column: Int),
            rowItem: TableRowItem,
            columnName: String,
            value: String?
        ) {
            guard let descriptor = editDescriptor(for: cell.column),
                  let onCommitEdit else {
                return
            }

            let formattedValue = value.map { Self.unformatEditorValue($0, descriptor: descriptor) }
            showEditorPopover(
                for: cell,
                descriptor: descriptor,
                originalValue: editorOriginalValue,
                currentValue: value,
                errorMessage: nil,
                isSaving: true
            )

            editorSaveTask?.cancel()
            editorSaveTask = Task { [weak self] in
                guard let self else { return }
                let result = await onCommitEdit(rowItem, columnName, formattedValue)
                await MainActor.run {
                    switch result {
                    case .success:
                        self.closeEditorPopover()
                    case .failure(let message):
                        self.showEditorPopover(
                            for: cell,
                            descriptor: descriptor,
                            originalValue: self.editorOriginalValue,
                            currentValue: value,
                            errorMessage: message,
                            isSaving: false
                        )
                    }
                }
            }
        }

        private func closePreviewPopover() {
            previewLoadTask?.cancel()
            previewLoadTask = nil
            previewRequestID = nil
            previewPopoverCell = nil
            let popover = previewPopover
            previewPopover = nil
            popover?.performClose(nil)
        }

        private func closeEditorPopover() {
            editorLoadTask?.cancel()
            editorSaveTask?.cancel()
            editorLoadTask = nil
            editorSaveTask = nil
            editorPopoverCell = nil
            editorOriginalValue = nil
            let popover = editorPopover
            editorPopover = nil
            popover?.performClose(nil)
        }

        private func makePreviewPopoverIfNeeded() -> NSPopover {
            if let previewPopover {
                return previewPopover
            }

            let popover = NSPopover()
            popover.behavior = .transient
            popover.animates = true
            popover.contentViewController = previewPopoverController
            popover.delegate = self
            self.previewPopover = popover
            return popover
        }

        private func makeEditorPopoverIfNeeded() -> NSPopover {
            if let editorPopover {
                return editorPopover
            }

            let popover = NSPopover()
            popover.behavior = .transient
            popover.animates = true
            popover.contentViewController = editorPopoverController
            popover.delegate = self
            self.editorPopover = popover
            return popover
        }

        private func popoverWidth(for column: Int) -> CGFloat {
            let baseWidth = tableView?.tableColumns[safe: column].map(\.width) ?? DataGridMetrics.defaultColumnWidth
            let proposed = baseWidth * 2.2
            let windowWidthLimit = tableView?.window.map { window in
                max(240, window.contentLayoutRect.width - (DataGridMetrics.popoverWindowPadding * 2))
            } ?? DataGridMetrics.popoverMaxWidth
            let maxWidth = min(DataGridMetrics.popoverMaxWidth, windowWidthLimit)
            let minWidth = min(DataGridMetrics.popoverMinWidth, maxWidth)
            return min(max(minWidth, proposed), maxWidth)
        }

        private func editorPopoverPlacement(
            for cell: (row: Int, column: Int),
            preferredHeight: CGFloat
        ) -> (edge: NSRectEdge, height: CGFloat) {
            guard let tableView,
                  let window = tableView.window else {
                return (.maxY, preferredHeight)
            }

            let cellRect = tableView.frameOfCell(atColumn: cell.column, row: cell.row)
            let cellRectInWindow = tableView.convert(cellRect, to: nil)
            let layoutRect = window.contentLayoutRect.insetBy(dx: 0, dy: DataGridMetrics.popoverWindowPadding)
            let availableAbove = max(0, layoutRect.maxY - cellRectInWindow.maxY)
            let availableBelow = max(0, cellRectInWindow.minY - layoutRect.minY)
            return Self.responsivePopoverPlacement(
                preferredHeight: preferredHeight,
                availableAbove: availableAbove,
                availableBelow: availableBelow
            )
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

        static func normalizeCellDisplayText(_ value: String) -> String {
            let normalizedNewlines = value
                .replacingOccurrences(of: "\r\n", with: "\n")
                .replacingOccurrences(of: "\r", with: "\n")
            return normalizedNewlines
                .replacingOccurrences(of: "\n", with: " ↩ ")
                .replacingOccurrences(of: "\t", with: "    ")
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

        static func formatEditorValue(_ value: String, descriptor: ColumnEditDescriptor) -> String {
            switch descriptor.editorKind {
            case .boolean:
                let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                if ["t", "1"].contains(normalized) {
                    return "true"
                }
                if ["f", "0"].contains(normalized) {
                    return "false"
                }
                return normalized
            case .textArea where descriptor.typeName == "json" || descriptor.typeName == "jsonb":
                return formatPopoverValue(value, columnTypeName: descriptor.typeName)
            case .enumeration, .textArea, .textField:
                return value
            }
        }

        static func unformatEditorValue(_ value: String, descriptor: ColumnEditDescriptor) -> String {
            switch descriptor.editorKind {
            case .boolean:
                let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                if ["t", "1"].contains(normalized) {
                    return "true"
                }
                if ["f", "0"].contains(normalized) {
                    return "false"
                }
                return normalized
            case .enumeration, .textArea, .textField:
                return value
            }
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

        static func editorPopoverHeight(descriptor: ColumnEditDescriptor, value: String) -> CGFloat {
            switch descriptor.editorKind {
            case .textArea:
                return min(max(280, popoverHeight(for: value) + 112), DataGridMetrics.popoverMaxHeight)
            case .enumeration:
                return 156
            case .boolean, .textField:
                return 142
            }
        }

        static func responsivePopoverPlacement(
            preferredHeight: CGFloat,
            availableAbove: CGFloat,
            availableBelow: CGFloat
        ) -> (edge: NSRectEdge, height: CGFloat) {
            let edge: NSRectEdge
            let availableHeight: CGFloat
            if availableBelow >= availableAbove {
                edge = .maxY
                availableHeight = availableBelow
            } else {
                edge = .minY
                availableHeight = availableAbove
            }

            let usableHeight = max(0, availableHeight - DataGridMetrics.popoverArrowClearance)
            guard usableHeight > 0 else {
                return (edge, min(preferredHeight, DataGridMetrics.popoverMaxHeight))
            }

            let minHeight = min(DataGridMetrics.popoverMinHeight, usableHeight)
            let height = max(minHeight, min(preferredHeight, usableHeight))
            return (edge, height)
        }
    }
}

private final class PaddedTableHeaderCell: NSTableHeaderCell {
    override init(textCell string: String) {
        super.init(textCell: string)
        configure()
    }

    required init(coder: NSCoder) {
        super.init(coder: coder)
        configure()
    }

    override func drawingRect(forBounds rect: NSRect) -> NSRect {
        rect.insetBy(dx: DataGridMetrics.cellHorizontalPadding, dy: 0).integral
    }

    override func drawInterior(withFrame cellFrame: NSRect, in controlView: NSView) {
        let titleRect = drawingRect(forBounds: cellFrame)
        guard titleRect.width > 0, titleRect.height > 0, !stringValue.isEmpty else { return }

        let text = stringValue as NSString
        let measuredRect = text.boundingRect(
            with: NSSize(width: titleRect.width, height: .greatestFiniteMagnitude),
            options: [.usesLineFragmentOrigin, .usesFontLeading, .truncatesLastVisibleLine],
            attributes: textAttributes
        )
        let drawRect = NSRect(
            x: titleRect.minX,
            y: floor(titleRect.midY - (measuredRect.height / 2)),
            width: titleRect.width,
            height: ceil(measuredRect.height)
        ).integral

        text.draw(
            with: drawRect,
            options: [.usesLineFragmentOrigin, .usesFontLeading, .truncatesLastVisibleLine],
            attributes: textAttributes
        )
    }

    private func configure() {
        font = .monospacedSystemFont(ofSize: 13, weight: .semibold)
        alignment = .left
        wraps = false
        usesSingleLineMode = true
        isScrollable = true
        truncatesLastVisibleLine = true
        lineBreakMode = .byTruncatingTail
    }

    private var textAttributes: [NSAttributedString.Key: Any] {
        let paragraphStyle = NSMutableParagraphStyle()
        paragraphStyle.alignment = alignment
        paragraphStyle.lineBreakMode = .byTruncatingTail

        return [
            .font: font ?? .monospacedSystemFont(ofSize: 13, weight: .semibold),
            .foregroundColor: NSColor.labelColor,
            .paragraphStyle: paragraphStyle,
        ]
    }
}
