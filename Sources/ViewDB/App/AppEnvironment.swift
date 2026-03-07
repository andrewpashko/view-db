import Foundation

struct AppEnvironment {
    let discoveryCoordinator: DiscoveryCoordinator
    let postgresRepository: PostgresRepository

    var catalogService: any CatalogService {
        postgresRepository
    }

    var queryService: any QueryService {
        postgresRepository
    }

    var cellEditingService: any CellEditingService {
        postgresRepository
    }

    static func live() -> AppEnvironment {
        let shell = ShellCommandRunner()
        let providers: [any DiscoveryProvider] = [
            BrewDiscoveryProvider(commandRunner: shell),
            PostgresAppDiscoveryProvider(),
            DockerDiscoveryProvider(commandRunner: shell),
        ]
        let coordinator = DiscoveryCoordinator(providers: providers)
        let credentialsStore = CredentialsStore()
        let pool = PostgresSessionPool(credentialsStore: credentialsStore)
        let repository = PostgresRepository(sessionPool: pool, instanceLookup: coordinator)

        return AppEnvironment(discoveryCoordinator: coordinator, postgresRepository: repository)
    }
}
