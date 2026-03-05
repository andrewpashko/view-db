import Foundation

struct DiscoveredInstance: Identifiable, Hashable, Codable, Sendable {
    let id: UUID
    let source: InstanceSource
    let displayName: String
    let host: String?
    let port: Int?
    let socketPath: String?
    let defaultUser: String?
    let defaultPassword: String?
    let defaultDatabase: String?
    let containerID: String?

    init(
        source: InstanceSource,
        displayName: String,
        host: String?,
        port: Int?,
        socketPath: String?,
        defaultUser: String? = nil,
        defaultPassword: String? = nil,
        defaultDatabase: String? = nil,
        containerID: String? = nil
    ) {
        self.source = source
        self.displayName = displayName
        self.host = host
        self.port = port
        self.socketPath = socketPath
        self.defaultUser = defaultUser
        self.defaultPassword = defaultPassword
        self.defaultDatabase = defaultDatabase
        self.containerID = containerID
        self.id = StableID.uuid(for: [
            source.rawValue,
            displayName,
            host ?? "",
            String(port ?? 0),
            socketPath ?? "",
            containerID ?? "",
        ].joined(separator: "|"))
    }

    var endpointKey: String {
        if let socketPath {
            return "socket:\(socketPath)"
        }
        return "tcp:\(host ?? "localhost"):\(port ?? 5432)"
    }

    var endpointLabel: String {
        if let socketPath {
            return socketPath
        }
        return "\(host ?? "localhost"):\(port ?? 5432)"
    }
}
