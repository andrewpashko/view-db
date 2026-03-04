import Observation
import SwiftUI

struct HomeView: View {
    @Bindable var router: AppRouter
    @State private var viewModel: HomeViewModel

    private let environment: AppEnvironment

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
                controls

                if viewModel.isLoading {
                    ProgressView("Discovering local PostgreSQL databases...")
                        .padding(.top, 8)
                }

                if let errorMessage = viewModel.errorMessage {
                    Text(errorMessage)
                        .font(.caption)
                        .foregroundStyle(.orange)
                }

                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 14) {
                        ForEach(viewModel.visibleGroups) { group in
                            VStack(alignment: .leading, spacing: 10) {
                                groupHeader(group)

                                ForEach(group.databases) { database in
                                    Button {
                                        router.open(database: database)
                                    } label: {
                                        DatabaseCardView(
                                            database: database,
                                            instance: group.instance
                                        )
                                    }
                                    .buttonStyle(.plain)
                                }

                                if group.databases.isEmpty {
                                    Text(group.warningMessage ?? "No databases found.")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                            }
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
            .padding(18)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .background(Color(nsColor: .windowBackgroundColor).ignoresSafeArea())
            .navigationTitle("ViewDB")
            .navigationDestination(for: DatabaseRef.self) { database in
                DatabaseView(
                    database: database,
                    environment: environment
                )
            }
            .task {
                viewModel.load()
            }
        }
    }

    private var controls: some View {
        controlsRow
    }

    private var controlsRow: some View {
        HStack(spacing: 10) {
            TextField("Search databases", text: $viewModel.searchText)
                .textFieldStyle(.roundedBorder)

            Toggle("Show system DBs", isOn: $viewModel.includeSystemDatabases)
                .toggleStyle(.switch)
                .onChange(of: viewModel.includeSystemDatabases) { _, _ in
                    viewModel.load()
                }

            Button {
                viewModel.refresh()
            } label: {
                Image(systemName: "arrow.clockwise")
            }
            .viewDBGlassButton()
        }
    }

    @ViewBuilder
    private func groupHeader(_ group: DatabaseGroup) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text("\(group.instance.source.displayName) - \(group.instance.displayName)")
                .font(.subheadline.bold())
            if let warning = group.warningMessage {
                Text(warning)
                    .font(.caption)
                    .foregroundStyle(.orange)
            }
        }
    }
}
