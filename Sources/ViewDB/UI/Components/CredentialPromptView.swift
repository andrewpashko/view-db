import SwiftUI

struct CredentialPromptView: View {
    let state: CredentialPromptState
    let onSubmit: (ConnectionCredentials) -> Void
    let onCancel: () -> Void

    @State private var username: String
    @State private var password: String = ""
    @State private var saveToKeychain = true

    init(state: CredentialPromptState, onSubmit: @escaping (ConnectionCredentials) -> Void, onCancel: @escaping () -> Void) {
        self.state = state
        self.onSubmit = onSubmit
        self.onCancel = onCancel
        _username = State(initialValue: state.defaultUsername)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Database Credentials")
                .font(.title3.bold())

            Text(state.reason)
                .font(.subheadline)
                .foregroundStyle(.secondary)

            TextField("Username", text: $username)
                .textFieldStyle(.roundedBorder)

            SecureField("Password", text: $password)
                .textFieldStyle(.roundedBorder)

            Toggle("Save in Keychain", isOn: $saveToKeychain)

            HStack {
                Button("Cancel") {
                    onCancel()
                }
                .keyboardShortcut(.cancelAction)

                Spacer()

                Button("Connect") {
                    onSubmit(
                        ConnectionCredentials(
                            username: username.trimmingCharacters(in: .whitespacesAndNewlines),
                            password: password,
                            saveToKeychain: saveToKeychain
                        )
                    )
                }
                .keyboardShortcut(.defaultAction)
                .viewDBGlassButton(prominent: true)
                .disabled(username.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || password.isEmpty)
            }
            .padding(.top, 4)
        }
        .padding(20)
        .frame(minWidth: 380)
    }
}
