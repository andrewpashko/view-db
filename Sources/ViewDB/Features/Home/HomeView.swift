import Observation
import SwiftUI

struct HomeView: View {
    @Bindable var router: AppRouter
    @State private var viewModel: HomeViewModel
    @FocusState private var isSearchFocused: Bool

    private let environment: AppEnvironment
    private let gridColumns = [
        GridItem(.adaptive(minimum: 260, maximum: 360), spacing: 12, alignment: .top),
    ]

    init(router: AppRouter, environment: AppEnvironment) {
        self.router = router
        self.environment = environment
        _viewModel = State(initialValue: HomeViewModel(
            discoveryCoordinator: environment.discoveryCoordinator,
            catalogService: environment.catalogService
        ))
    }

    var body: some View {
        NavigationStack(path: $router.path) {
            VStack(alignment: .leading, spacing: 16) {
                topBar
                    .padding(.horizontal, 18)

                if viewModel.isLoading {
                    ProgressView("Discovering local PostgreSQL databases...")
                        .padding(.top, 8)
                        .padding(.horizontal, 18)
                }

                if let errorMessage = viewModel.errorMessage {
                    Text(errorMessage)
                        .font(.caption)
                        .foregroundStyle(.orange)
                        .padding(.horizontal, 18)
                }

                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 14) {
                        ForEach(viewModel.visibleGroups) { group in
                            VStack(alignment: .leading, spacing: 12) {
                                groupHeader(group)

                                if group.databases.isEmpty {
                                    Text(emptyStateMessage(for: group))
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                        .padding(.leading, 4)
                                } else if #available(macOS 26.0, *) {
                                    GlassEffectContainer(spacing: 12) {
                                        databaseGrid(for: group)
                                    }
                                } else {
                                    databaseGrid(for: group)
                                }

                                if let warning = group.warningMessage, !group.databases.isEmpty {
                                    Text(warning)
                                        .font(.caption)
                                        .foregroundStyle(.orange)
                                }
                            }
                        }
                    }
                    .padding(.horizontal, 18)
                    .padding(.bottom, 18)
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
                .simultaneousGesture(
                    TapGesture().onEnded {
                        isSearchFocused = false
                    }
                )
            }
            .padding(.top, 18)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .background {
                LinearGradient(
                    colors: [
                        Color(nsColor: .windowBackgroundColor),
                        Color(nsColor: .controlBackgroundColor).opacity(0.82),
                    ],
                    startPoint: .topLeading,
                    endPoint: .bottomTrailing
                )
                .ignoresSafeArea()
            }
            .navigationTitle("ViewDB")
            .navigationDestination(for: DatabaseRef.self) { database in
                DatabaseView(
                    database: database,
                    environment: environment
                )
            }
            .toolbarBackgroundVisibility(.hidden, for: .windowToolbar)
            .task {
                viewModel.load()
            }
        }
    }

    private var topBar: some View {
        HStack(spacing: 10) {
            searchField
            toolbarControlButtons
        }
    }

    private var searchField: some View {
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)
                .font(.system(size: 16, weight: .semibold))

            TextField("Search databases", text: $viewModel.searchText)
                .textFieldStyle(.plain)
                .focused($isSearchFocused)
                .onSubmit {
                    isSearchFocused = false
                }

            if !viewModel.searchText.isEmpty {
                Button {
                    viewModel.searchText = ""
                    isSearchFocused = true
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
            isSearchFocused = true
        }
    }

    @ViewBuilder
    private var toolbarControlButtons: some View {
        if #available(macOS 26.0, *) {
            GlassEffectContainer(spacing: 8) {
                controlButtons
            }
        } else {
            controlButtons
        }
    }

    private var controlButtons: some View {
        HStack(spacing: 8) {
            hiddenDatabasesButton
            refreshButton
        }
    }

    private var hiddenDatabasesButton: some View {
        ZStack(alignment: .topTrailing) {
            Button {
                isSearchFocused = false
                viewModel.showHiddenDatabases.toggle()
            } label: {
                Image(systemName: viewModel.showHiddenDatabases ? "eye" : "eye.slash")
                    .font(.system(size: 16, weight: .semibold))
                    .frame(width: 44, height: 44)
                    .contentShape(Circle())
            }
            .disabled(!viewModel.showHiddenDatabases && viewModel.hiddenDatabaseCount == 0)
            .help(viewModel.showHiddenDatabases ? "Hide hidden databases" : "Show hidden databases")
            .buttonStyle(.plain)
            .viewDBGlassCard(interactive: true, cornerRadius: 22)

            if viewModel.hiddenDatabaseCount > 0 {
                Text("\(viewModel.hiddenDatabaseCount)")
                    .font(.caption2.monospacedDigit().bold())
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(.blue.opacity(0.3), in: Capsule())
                    .offset(x: 8, y: -8)
                    .allowsHitTesting(false)
                    .zIndex(3)
            }
        }
        .zIndex(2)
    }

    private var refreshButton: some View {
        Button {
            isSearchFocused = false
            viewModel.refresh()
        } label: {
            Image(systemName: "arrow.clockwise")
                .font(.system(size: 16, weight: .semibold))
                .frame(width: 44, height: 44)
                .contentShape(Circle())
        }
        .buttonStyle(.plain)
        .viewDBGlassCard(interactive: true, cornerRadius: 22)
        .zIndex(1)
    }

    @ViewBuilder
    private func groupHeader(_ group: DatabaseGroup) -> some View {
        HStack(alignment: .center, spacing: 10) {
            DatabaseHeaderChipView(
                title: group.instance.displayName,
                subtitle: "\(group.instance.source.displayName) - \(group.instance.endpointLabel)"
            )

            Spacer(minLength: 6)

            Text("\(group.databases.count) \(group.databases.count == 1 ? "DB" : "DBs")")
                .font(.caption.bold())
                .foregroundStyle(.secondary)
                .padding(.horizontal, 10)
                .padding(.vertical, 5)
                .background(Color.primary.opacity(0.08), in: Capsule())
        }
    }

    private func databaseGrid(for group: DatabaseGroup) -> some View {
        LazyVGrid(columns: gridColumns, alignment: .leading, spacing: 12) {
            ForEach(group.databases) { database in
                DatabaseCardView(
                    database: database,
                    instance: group.instance,
                    isHidden: viewModel.isDatabaseHidden(database),
                    onOpen: {
                        isSearchFocused = false
                        router.open(database: database)
                    },
                    onToggleHidden: {
                        isSearchFocused = false
                        viewModel.toggleDatabaseVisibility(database)
                    }
                )
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func emptyStateMessage(for group: DatabaseGroup) -> String {
        let hiddenCount = viewModel.hiddenDatabaseCount(for: group.instance.id)
        if hiddenCount > 0 && !viewModel.showHiddenDatabases {
            return "All databases are hidden. Use \"Show hidden\" to manage them."
        }
        return group.warningMessage ?? "No databases found."
    }
}
