# 0013. Be secure by default, and stop there

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D12
- Relates to: PLAN.md section 2 D12, PLAN.md section 10, SECURITY.md, issue #4, issue #16

## Context

messq is an internal broker on a trusted-ish network, run by a team with no dedicated security engineer. The threat posture that fits is: no unauthenticated network path, no secrets in logs, no surprises in the default configuration.

The security-engineer plan proposed considerably more: a hash-chained audit trail, Ed25519 checkpoints, a grant-coverage decision procedure and SLSA provenance. Each is a real design and each is correct for a different customer.

## Decision

The default listener is a Unix socket at mode 0660 with a `messq` group. Filesystem permissions are the access control list and the whole story for local use. The data directory is 0700 and the database file is 0600, verified at startup, and the daemon refuses to run otherwise.

TCP requires authentication. Tokens live in a 0600 file, stored as the SHA-256 of a 256-bit random secret, compared in constant time, and reloaded on SIGHUP. Each token carries roles from `publish`, `consume` and `admin`, scoped to stream-name patterns. **A non-loopback bind without authentication is a fatal startup error.**

Token identifiers appear in logs; secrets implement `slog.LogValuer` and are therefore structurally unloggable. Payloads never enter logs, enforced by a redaction type, a `ReplaceAttr` denylist and a CI canary test that greps all output for a published canary string.

There is no TLS in core at 1.0. Terminate TLS in a reverse proxy into the Unix socket, or use WireGuard or Tailscale. That is one documented paragraph instead of a certificate subsystem.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| A hash-chained, tamper-evident audit trail with Ed25519 checkpoints | It is the right design when the audit trail is evidence against an insider, and the events table is already an audit trail. | It is the wrong tax bracket for the intended user. The in-transaction events table is the audit trail this product promises, and tamper-evidence protects against a threat this deployment model does not have. |
| A full role-based access control engine | It scales to many teams and fine-grained permissions. | Three roles scoped by stream pattern is about a hundred lines and covers the deployments messq targets. An RBAC engine is a subsystem with its own bugs and its own documentation. |
| Native TLS in core at 1.0 | Operators expect it, and the standard library makes it feasible. | It brings certificate loading, rotation, reload, cipher policy and a support surface. A reverse proxy into the Unix socket already solves it, and native TLS is a phase 2 item with a design of its own. |
| Bind TCP without authentication when the operator asks for it | It is convenient in a lab, and the operator is an adult. | An unauthenticated broker on a network is a data-loss incident waiting for a scan. `--dev` covers the lab case on loopback; a non-loopback bind without authentication fails at startup. |
| SLSA provenance and a signed release ceremony at 1.0 | Supply-chain integrity is real and increasingly expected. | It is process weight before there is a release cadence. `govulncheck` runs in CI and a `gitleaks` rule guards the token prefix; provenance can be added when releases exist to provide it for. |

## Consequences

The default deployment has no network attack surface at all. The socket is the boundary, and the operating system enforces it.

A secret cannot reach a log by accident, because the type refuses to render. The canary test proves the payload half of that claim on every run, so it is a gate rather than a promise.

The shipped systemd unit is hardened: `Type=notify`, `ProtectSystem=strict`, `NoNewPrivileges`, `StateDirectory=messq`, a syscall filter and an empty capability set. `--exec` is a CLI-side feature only; the daemon has no exec capability.

The cost is that a TCP deployment needs a proxy or a private network for encryption, and the documentation has to say so clearly rather than leaving it implied.

## Revisit trigger

Native TLS ships in phase 2. Anything beyond that reopens only on a deployment model messq does not target today, such as a multi-tenant or internet-facing listener.
