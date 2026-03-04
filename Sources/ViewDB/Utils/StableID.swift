import CryptoKit
import Foundation

enum StableID {
    static func uuid(for value: String) -> UUID {
        let hash = SHA256.hash(data: Data(value.utf8))
        var bytes = Array(hash.prefix(16))

        // RFC 4122 version 4, variant 1 bits.
        bytes[6] = (bytes[6] & 0x0F) | 0x40
        bytes[8] = (bytes[8] & 0x3F) | 0x80

        let tuple = (
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]
        )
        return UUID(uuid: tuple)
    }
}
