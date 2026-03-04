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
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                toolbarControls
            }
        }
        .sheet(isPresented: $viewModel.isSQLSheetPresented) {
            SQLSheetView(
                sqlText: $viewModel.sqlText,
                rowPage: viewModel.sqlRowPage,
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
                                            .font(.system(.body, design: .monospaced))
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

            VStack(alignment: .leading, spacing: 12) {
                header

                if viewModel.isLoadingRows {
                    ProgressView("Loading rows...")
                        .padding(.vertical, 10)
                }

                if let selectedTable = viewModel.selectedTable {
                    tableContent(selectedTable)
                    pagingControls
                } else {
                    ContentUnavailableView(
                        "No Table Selected",
                        systemImage: "tablecells",
                        description: Text("Choose a table from the sidebar.")
                    )
                }
            }
            .padding(16)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    @ViewBuilder
    private var header: some View {
        let subtitle = viewModel.instance?.endpointLabel ?? "Local instance"
        if #available(macOS 26.0, *) {
            GlassEffectContainer(spacing: 8) {
                DatabaseHeaderChipView(
                    title: database.name,
                    subtitle: subtitle
                )
            }
        } else {
            DatabaseHeaderChipView(
                title: database.name,
                subtitle: subtitle
            )
        }
    }

    @ViewBuilder
    private func tableContent(_ table: TableRef) -> some View {
        Text(table.fullName)
            .font(.system(.headline, design: .monospaced))

        if viewModel.rowPage.columns.isEmpty {
            ContentUnavailableView(
                "No Rows",
                systemImage: "tray",
                description: Text("The table returned no data.")
            )
        } else {
            DataGridView(columns: viewModel.rowPage.columns, rows: viewModel.tableRows)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    private var pagingControls: some View {
        HStack(spacing: 10) {
            Text("Rows: \(viewModel.rowPage.rows.count)")
                .font(.caption)
                .foregroundStyle(.secondary)

            Spacer()

            if #available(macOS 26.0, *) {
                GlassEffectContainer(spacing: 8) {
                    pagingButtons
                }
            } else {
                pagingButtons
            }
        }
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
            Button("Previous") {
                viewModel.fetchPreviousPage()
            }
            .viewDBGlassButton()
            .disabled(viewModel.rowPage.offset == 0 || viewModel.isLoadingRows)

            Button("Next") {
                viewModel.fetchNextPage()
            }
            .viewDBGlassButton(prominent: true)
            .disabled(!viewModel.rowPage.hasNext || viewModel.isLoadingRows)
        }
    }
}
