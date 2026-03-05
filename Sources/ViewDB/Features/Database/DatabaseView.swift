import SwiftUI

struct DatabaseView: View {
    let database: DatabaseRef
    let environment: AppEnvironment

    @State private var viewModel: DatabaseViewModel

    init(database: DatabaseRef, environment: AppEnvironment) {
        self.database = database
        self.environment = environment
        _viewModel = State(initialValue: DatabaseViewModel(
            database: database,
            instanceLookup: environment.discoveryCoordinator,
            catalogService: environment.catalogService,
            queryService: environment.queryService,
            credentialService: environment.postgresRepository
        ))
    }

    var body: some View {
        NavigationSplitView {
            sidebar
        } detail: {
            detail
        }
        .navigationTitle(database.name)
        .toolbarBackgroundVisibility(.hidden, for: .windowToolbar)
        .toolbar {
            ToolbarItem(placement: .principal) {
                toolbarTitleLabel
            }
            ToolbarItem(placement: .primaryAction) {
                toolbarControls
            }
        }
        .sheet(isPresented: $viewModel.isSQLSheetPresented) {
            SQLSheetView(
                sqlText: $viewModel.sqlText,
                rowPage: viewModel.sqlRowPage,
                rows: viewModel.sqlRows,
                isRunning: viewModel.isRunningSQL,
                errorMessage: viewModel.sqlErrorMessage,
                onRun: viewModel.runSQL
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
            TextField("Search tables", text: $viewModel.tableSearch)
                .textFieldStyle(.roundedBorder)
                .padding(.horizontal, 12)
                .padding(.top, 12)

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
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .navigationSplitViewColumnWidth(min: 240, ideal: 280)
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
                    .padding(.bottom, 16)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var toolbarTitleLabel: some View {
        HStack(spacing: 8) {
            Image(systemName: "cylinder.split.1x2")
                .font(.headline)
            Text(database.name)
                .font(.headline.weight(.semibold))
                .lineLimit(1)

            HStack(spacing: 6) {
                ProgressView()
                    .controlSize(.small)
                Text("Loading…")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .frame(width: 86, alignment: .leading)
            .opacity(viewModel.isLoadingTables ? 1 : 0)
            .accessibilityElement(children: .combine)
            .accessibilityLabel("Loading tables")
            .accessibilityHidden(!viewModel.isLoadingTables)
        }
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
                DataGridView(columns: viewModel.rowPage.columns, rows: viewModel.tableRows)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .transaction { transaction in
                        transaction.animation = nil
                    }
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

    @ViewBuilder
    private var toolbarControls: some View {
        if #available(macOS 26.0, *) {
            GlassEffectContainer(spacing: 8) {
                toolbarButtons
            }
        } else {
            toolbarButtons
        }
    }

    private var toolbarButtons: some View {
        HStack(spacing: 8) {
            Button("Run SQL") {
                viewModel.openSQLSheet()
            }
            .viewDBGlassButton()

            Button {
                viewModel.refresh()
            } label: {
                Image(systemName: "arrow.clockwise")
            }
            .viewDBGlassButton()
        }
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
        let count = viewModel.rowPage.rows.count
        guard count > 0 else { return "0-0" }

        let start = viewModel.rowPage.offset + 1
        let end = viewModel.rowPage.offset + count
        return "\(start.formatted())-\(end.formatted())"
    }

    private var totalEntriesText: String {
        if let total = viewModel.totalRowCount {
            return total.formatted()
        }
        return viewModel.isLoadingRows ? "…" : "—"
    }

}
