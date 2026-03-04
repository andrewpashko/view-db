import Foundation

struct ShellCommandResult: Sendable {
    let status: Int32
    let stdout: String
    let stderr: String

    var succeeded: Bool {
        status == 0
    }
}

protocol ShellCommandRunning: Sendable {
    func run(_ command: String, arguments: [String]) async -> ShellCommandResult
}

actor ShellCommandRunner: ShellCommandRunning {
    func run(_ command: String, arguments: [String] = []) async -> ShellCommandResult {
        await withCheckedContinuation { continuation in
            let process = Process()
            let stdoutPipe = Pipe()
            let stderrPipe = Pipe()

            process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
            process.arguments = [command] + arguments
            process.standardOutput = stdoutPipe
            process.standardError = stderrPipe

            process.terminationHandler = { process in
                let outData = stdoutPipe.fileHandleForReading.readDataToEndOfFile()
                let errData = stderrPipe.fileHandleForReading.readDataToEndOfFile()
                let out = String(data: outData, encoding: .utf8) ?? ""
                let err = String(data: errData, encoding: .utf8) ?? ""
                continuation.resume(returning: ShellCommandResult(status: process.terminationStatus, stdout: out, stderr: err))
            }

            do {
                try process.run()
            } catch {
                continuation.resume(
                    returning: ShellCommandResult(
                        status: -1,
                        stdout: "",
                        stderr: error.localizedDescription
                    )
                )
            }
        }
    }
}
