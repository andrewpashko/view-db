import SwiftUI

struct DatabaseView: View {
    let database: DatabaseRef
    let environment: AppEnvironment

    @State private var viewModel: DatabaseViewModel
    @FocusState private var isTableSearchFocused: Bool

    init(database: DatabaseRef, environment: AppEnvironment) {
        self.database = database
        self.environment = environment
        _viewModel = State(initialValue: DatabaseViewModel(
            database: database,
            instanceLookup: environment.discoveryCoordinator,
            catalogService: environment.catalogService,
            queryService: environment.queryService,
            cellEditingService: environment.cellEditingService,
            credentialService: environment.postgresRepository
        ))
    }

    var body: some View {
        NavigationSplitView {
            sidebar
        } detail: {
            detail
        }
        .simultaneousGesture(
            TapGesture().onEnded {
                isTableSearchFocused = false
            }
        )
        .navigationTitle(database.name)
        .toolbar(removing: .title)
        .toolbarBackgroundVisibility(.hidden, for: .windowToolbar)
        .toolbar {
            ToolbarItem(placement: .principal) {
                toolbarTitleLabel
            }
            ToolbarItem(placement: .primaryAction) {
                toolbarActions
            }
        }
        .sheet(isPresented: $viewModel.isSQLSheetPresented) {
            SQLSheetView(
                sqlText: $viewModel.sqlText,
                rowPage: viewModel.sqlRowPage,
                rows: viewModel.sqlRows,
                isRunning: viewModel.isRunningSQL,
                errorMessage: viewModel.sqlErrorMessage,
                onRun: viewModel.runSQL,
                onClose: {
                    viewModel.isSQLSheetPresented = false
                }
            )
        }
        .sheet(item: $viewModel.credentialPrompt) { state in
            CredentialPromptView(
                state: state,
                onSubmit: viewModel.submitCredentials,
                onCancel: {
                    viewModel.credentialPrompt = nil
                }
            )
        }
        .task {
            viewModel.load()
        }
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 10) {
            tableSearchField

            ScrollView {
                LazyVStack(alignment: .leading, spacing: 10) {
                    ForEach(viewModel.tableSections) { section in
                        VStack(alignment: .leading, spacing: 4) {
                            Text(section.schema)
                                .font(.system(.headline, design: .monospaced))
                                .foregroundStyle(.secondary)
                                .padding(.horizontal, 8)
                                .padding(.top, 2)

                            ForEach(section.tables) { table in
                                Button {
                                    viewModel.selectTable(table)
                                } label: {
                                    HStack {
                                        Text(table.name)
                                            .font(.system(.callout, design: .monospaced))
                                        Spacer(minLength: 0)
                                    }
                                    .padding(.horizontal, 8)
                                    .padding(.vertical, 6)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(
                                        RoundedRectangle(cornerRadius: 8)
                                            .fill(viewModel.selectedTable?.id == table.id ? Color.accentColor.opacity(0.16) : Color.clear)
                                    )
                                    .contentShape(Rectangle())
                                }
                                .buttonStyle(.plain)
                            }
                        }
                    }
                }
                .padding(.horizontal, 12)
                .padding(.bottom, 12)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)

            if let errorMessage = viewModel.errorMessage {
                Text(errorMessage)
                    .font(.caption)
                    .foregroundStyle(.orange)
                .padding(.horizontal, 12)
                .padding(.bottom, 12)
            }
        }
        .simultaneousGesture(
            TapGesture().onEnded {
                isTableSearchFocused = false
            }
        )
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .navigationSplitViewColumnWidth(min: 240, ideal: 280)
    }

    private var tableSearchField: some View {
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)
                .font(.system(size: 16, weight: .semibold))

            TextField("Search tables", text: $viewModel.tableSearch)
                .textFieldStyle(.plain)
                .focused($isTableSearchFocused)
                .onSubmit {
                    isTableSearchFocused = false
                }

            if !viewModel.tableSearch.isEmpty {
                Button {
                    viewModel.tableSearch = ""
                    isTableSearchFocused = true
                } label: {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Clear search")
            }
        }
        .padding(.horizontal, 14)
        .frame(height: 44)
        .frame(maxWidth: .infinity)
        .viewDBGlassCard(interactive: true, cornerRadius: 22)
        .overlay(
            RoundedRectangle(cornerRadius: 22)
                .strokeBorder(Color.secondary.opacity(0.18), lineWidth: 1)
        )
        .contentShape(Rectangle())
        .onTapGesture {
            isTableSearchFocused = true
        }
        .padding(.horizontal, 12)
        .padding(.top, 12)
    }

    private var detail: some View {
        ZStack {
            Color(nsColor: .controlBackgroundColor).opacity(0.94)

            if let selectedTable = viewModel.selectedTable {
                tableContent(selectedTable)
                    .padding(.horizontal, 16)
                    .padding(.top, 16)
                    .padding(.bottom, 16)
            } else {
                ContentUnavailableView(
                    "No Table Selected",
                    systemImage: "tablecells",
                    description: Text("Choose a table from the sidebar.")
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .safeAreaInset(edge: .bottom, spacing: 0) {
            if viewModel.selectedTable != nil {
                pagingControls
                    .padding(.horizontal, 16)
                    .padding(.bottom, 12)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var toolbarTitleLabel: some View {
        HStack(spacing: 8) {
            Color.clear
                .frame(width: 18, height: 14)
                .accessibilityHidden(true)

            Image(systemName: "cylinder.split.1x2")
                .font(.headline)
            Text(database.name)
                .font(.headline.weight(.semibold))
                .lineLimit(1)
                .truncationMode(.middle)

            ProgressView()
                .controlSize(.small)
                .frame(width: 14, height: 14)
                .padding(.horizontal, 2)
                .opacity(viewModel.isLoadingTables ? 1 : 0)
                .accessibilityLabel("Loading tables")
                .accessibilityHidden(!viewModel.isLoadingTables)
        }
        .padding(.horizontal, 16)
        .fixedSize(horizontal: true, vertical: false)
    }

    @ViewBuilder
    private func tableContent(_ table: TableRef) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 10) {
                Text(table.fullName)
                    .font(.system(.headline, design: .monospaced))
                    .lineLimit(1)
                    .truncationMode(.middle)

                HStack(spacing: 6) {
                    ProgressView()
                        .controlSize(.small)
                    Text("Loading…")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(width: 86, alignment: .leading)
                .opacity(viewModel.isLoadingRows ? 1 : 0)
                .accessibilityElement(children: .combine)
                .accessibilityLabel("Loading rows")
                .accessibilityHidden(!viewModel.isLoadingRows)

                Spacer(minLength: 0)
            }
            .frame(maxWidth: .infinity, minHeight: 24, alignment: .leading)

            if viewModel.rowPage.columns.isEmpty {
                ContentUnavailableView(
                    "No Rows",
                    systemImage: "tray",
                    description: Text("The table returned no data.")
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                TableGridSection(
                    columns: viewModel.rowPage.columns,
                    columnTypeNames: viewModel.rowPage.columnTypeNames,
                    columnEditDescriptors: viewModel.columnEditDescriptors,
                    rows: viewModel.tableRows,
                    activeSort: viewModel.activeSort,
                    sortableColumns: viewModel.sortableColumns,
                    onToggleSort: { columnName in
                        viewModel.toggleSort(column: columnName)
                    },
                    onRequestFullValue: { rowIdentity, columnName in
                        await viewModel.fetchFullCellValue(rowIdentity: rowIdentity, columnName: columnName)
                    },
                    onBeginEdit: { row, columnName in
                        await viewModel.beginCellEdit(row: row, columnName: columnName)
                    },
                    onCommitEdit: { row, columnName, value in
                        await viewModel.saveCellEdit(row: row, columnName: columnName, value: value)
                    }
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var pagingControls: some View {
        HStack(spacing: 12) {
            rowsPerPageControl

            Divider()
                .frame(height: 28)

            pagingMetric(icon: "doc.text", title: "Page", value: "\(viewModel.currentPage) / \(viewModel.totalPages)")
            pagingMetric(icon: "line.3.horizontal", title: "Showing", value: visibleRangeText)
            pagingMetric(icon: "sum", title: "Total Entries", value: totalEntriesText)

            Spacer(minLength: 12)

            if #available(macOS 26.0, *) {
                GlassEffectContainer(spacing: 8) {
                    pagingButtons
                }
            } else {
                pagingButtons
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .viewDBGlassCard(cornerRadius: 12)
        .overlay {
            RoundedRectangle(cornerRadius: 12)
                .strokeBorder(Color.secondary.opacity(0.18), lineWidth: 1)
        }
    }

    private var rowsPerPageBinding: Binding<Int> {
        Binding(
            get: { viewModel.rowsPerPage },
            set: { viewModel.updateRowsPerPage($0) }
        )
    }

    private var runSQLButton: some View {
        Button {
            viewModel.openSQLSheet()
        } label: {
            Text("Run SQL")
                .font(.system(size: 14, weight: .semibold))
                .padding(.horizontal, 10)
                .frame(height: 34)
        }
        .buttonStyle(.plain)
        .help("Run SQL")
        .accessibilityLabel("Run SQL")
    }

    private var refreshButton: some View {
        Button {
            viewModel.refresh()
        } label: {
            Image(systemName: "arrow.clockwise")
                .font(.system(size: 16, weight: .semibold))
                .frame(width: 34, height: 34)
                .contentShape(Circle())
        }
        .buttonStyle(.plain)
        .help("Reload")
        .accessibilityLabel("Reload")
    }

    private var toolbarActions: some View {
        HStack(spacing: 0) {
            runSQLButton

            Rectangle()
                .fill(Color.secondary.opacity(0.22))
                .frame(width: 1, height: 26)
                .padding(.horizontal, 2)

            refreshButton
        }
        .padding(.horizontal, 2)
        .padding(.vertical, 2)
        .frame(height: 36)
        .viewDBGlassCard(interactive: true, cornerRadius: 18)
        .clipShape(.capsule)
        .fixedSize(horizontal: true, vertical: true)
    }

    private var pagingButtons: some View {
        HStack(spacing: 8) {
            Button {
                viewModel.fetchPreviousPage()
            } label: {
                Label("Previous", systemImage: "chevron.left")
            }
            .viewDBGlassButton()
            .disabled(viewModel.rowPage.offset == 0 || viewModel.isLoadingRows)

            Button {
                viewModel.fetchNextPage()
            } label: {
                Label("Next", systemImage: "chevron.right")
            }
            .viewDBGlassButton(prominent: true)
            .disabled(!viewModel.rowPage.hasNext || viewModel.isLoadingRows)
        }
    }

    private var rowsPerPageControl: some View {
        HStack(spacing: 8) {
            Text("Rows")
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)

            Picker("Rows per page", selection: rowsPerPageBinding) {
                ForEach(viewModel.rowsPerPageOptions, id: \.self) { value in
                    Text("\(value)").tag(value)
                }
            }
            .labelsHidden()
            .pickerStyle(.menu)
            .frame(width: 88)
        }
        .padding(.horizontal, 6)
    }

    private func pagingMetric(icon: String, title: String, value: String) -> some View {
        HStack(spacing: 6) {
            Image(systemName: icon)
                .font(.caption2)
                .foregroundStyle(.secondary)

            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)

            Text(value)
                .font(.system(.caption, design: .monospaced).weight(.semibold))
                .foregroundStyle(.primary)
        }
        .accessibilityElement(children: .combine)
    }

    private var visibleRangeText: String {
        let count = viewModel.tableRows.count
        guard count > 0 else { return "0-0" }

        let start = viewModel.rowPage.offset + 1
        let end = viewModel.rowPage.offset + count
        return "\(start.formatted())-\(end.formatted())"
    }

    private var totalEntriesText: String {
        if let total = viewModel.totalRowCount {
            return total.formatted()
        }
        return (viewModel.isLoadingRows || viewModel.isLoadingCount) ? "…" : "—"
    }

}

private struct TableGridSection: View {
    let columns: [String]
    let columnTypeNames: [String]
    let columnEditDescriptors: [ColumnEditDescriptor]
    let rows: [TableRowItem]
    let activeSort: TableSort?
    let sortableColumns: Set<String>
    let onToggleSort: (String) -> Void
    let onRequestFullValue: DataGridView.FullValueProvider
    let onBeginEdit: DataGridView.BeginEditValueProvider
    let onCommitEdit: DataGridView.CommitEditProvider

    var body: some View {
        DataGridView(
            columns: columns,
            columnTypeNames: columnTypeNames,
            columnEditDescriptors: columnEditDescriptors,
            rows: rows,
            activeSort: activeSort,
            sortableColumns: sortableColumns,
            onToggleSort: onToggleSort,
            onRequestFullValue: onRequestFullValue,
            onBeginEdit: onBeginEdit,
            onCommitEdit: onCommitEdit
        )
        .transaction { transaction in
            transaction.animation = nil
        }
    }
}
