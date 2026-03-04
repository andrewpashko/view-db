import Foundation

extension String {
    var trimmedLines: [String] {
        self
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    var singleLineTrimmed: String {
        trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
