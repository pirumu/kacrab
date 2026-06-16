# Security Policy

`kacrab` is pre-release and not ready for production use. It is intended to
remain 100% pure Rust, with no C Kafka client bindings and no unsafe code in
the workspace.

Even so, security reports are welcome, especially for issues involving protocol
parsing, malformed broker responses, TLS/SASL handling, panic-on-input bugs,
resource exhaustion, or incorrect error propagation.

## Supported Versions

There are no supported stable releases yet.

Security fixes are handled on `main` until the project starts publishing
supported release lines.

## Reporting a Vulnerability

Please do not open a public issue for a suspected vulnerability.

Use GitHub private vulnerability reporting if it is enabled for the repository.
If it is not enabled, contact the maintainer through the repository owner
profile and include:

- A short description of the issue.
- Steps to reproduce.
- A minimal input, broker behavior, or test case if available.
- Expected impact.
- Whether the issue is already public.

## Scope

In scope:

- Memory safety issues in safe Rust boundaries caused by this project.
- Any proposed dependency or implementation path that would introduce native
  Kafka client bindings or unsafe code.
- Panics or hangs triggered by untrusted network input.
- Incorrect parsing or validation of Kafka protocol data.
- Authentication, TLS, or transaction handling defects.
- Unbounded memory or task growth from broker-controlled input.

Out of scope:

- Vulnerabilities in an unsupported local Kafka deployment.
- Issues that require modifying the local build environment.
- General pre-release missing features without a concrete security impact.
