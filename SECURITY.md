# Security policy

## Reporting a vulnerability

Report privately through [GitHub private vulnerability reporting](https://github.com/a-holm/messq/security/advisories/new). That form is the disclosure address; it is private to the maintainers and does not create a public issue.

Do not open a public issue, a pull request, or a discussion for a suspected vulnerability.

Include the messq version (`messq version --output json`), the platform, a reproduction, and the impact you believe it has.

## What to expect

| Stage | Target |
|---|---|
| Acknowledgement | 3 working days |
| Initial assessment | 10 working days |
| Fix or mitigation published | within the 90-day disclosure window |

messq follows a 90-day coordinated disclosure policy. Details become public when a fix ships or 90 days after the report, whichever comes first. A reporter who wants to publish earlier is asked to coordinate the date.

There is no bug bounty.

## Supported versions

| Version | Supported |
|---|---|
| `main` | Yes |

Released versions get their own row from the first tag onwards.

## Threat posture (issue #16)

What messq claims, mechanically:

- **Local trust is the default boundary.** The Unix socket's filesystem
  permissions are the ACL for local clients (D12); TCP is opt-in, loopback-only
  unless authentication exists, and a public bind without `--auth-file` refuses
  startup (`EX_CONFIG`, exit 78) before anything binds.
- **Bearer credentials are SHA-256 hashes of the whole presented string**
  (`msq1_<id>_<secret>`, prefix included), compared with `crypto/subtle`.
  A slow KDF (argon2/bcrypt) was deliberately rejected for v1: secrets are
  256-bit crypto/rand values — not guessable by offline grinding anyway — while
  a memory-hard KDF hands every UNAUTHENTICATED client a CPU/memory amplification
  vector against the daemon itself. The control here is rate-free: constant-time
  compare, decoy digest so unknown-id costs the same as wrong-secret, and denials
  as observable facts (`messq_auth_denied_total{reason}`, auth.denied events).
- **Secrets are structurally unloggable**: `auth.Secret` renders REDACTED through
  every formatting path; `[Secret.Reveal]` is banned by lint outside internal/auth
  and pkg/client; a PR-lane canary greps raw credential bytes across captured
  output surfaces each run (a gate, not a promise). The denial observables
  (`messq_auth_denied_total{reason}`, auth.denied events with actor attribution)
  ship with the bearer middleware lane (#15); their reason label set is closed and
  never contains token ids.
- **Token files are data, not config** (D8): reload semantics keep them out of the
  flag surface entirely.
- **No TLS in core at 1.0.** Terminate TLS in your reverse proxy or tunnel into the
  Unix socket (ADR-0013). `--dev` never relaxes the public-bind refusal.

Known limitations, named honestly:

1. Transport to TCP peers is cleartext HTTP unless an external proxy terminates
   TLS; bearer credentials on a hostile network are readable until then (#40).
2. One auth file means grant granularity stops at stream patterns per token:
   no per-consumer grants, no expiry, no revocation list — rotate by replacing
   the whole file entry today (PLAN D12 rejects more for v1).
3. `messq auth add` prints the credential exactly once; after that the only
   copies live where the operator put them. Lost credentials require re-adding.

## Scope

The threat model that says which properties messq claims, and against whom, is tracked in issue #16.
