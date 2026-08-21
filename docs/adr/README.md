# Architecture decision records

An ADR records one decision that is expensive to reverse: what was decided, the alternatives, and the consequences the project accepts. Decisions that are cheap to change live in the code.

## Numbering

Files are named `NNNN-kebab-case-title.md` with a four-digit, zero-padded number. Numbers are allocated in the order decisions are taken, are contiguous, and are never reused. `0000-template.md` is the template and is not a decision.

## Required fields

Every record carries four fields under its title, and `internal/docsguard` fails the build when one is missing or malformed.

- `Status`: `Proposed`, `Accepted`, `Superseded by NNNN` or `Deprecated`. A superseded record stays in place with its status changed, and the replacement explains itself. A record is never edited to describe a later decision.
- `Date`: ISO `YYYY-MM-DD`.
- `Adjudicates`: the decision of PLAN.md section 2 this record settles, written `D1` through `D15`, or `none` for a record that settles something the plan did not adjudicate. Each of D1 through D15 is claimed by exactly one record.
- `Relates to`: the plan sections and issues the record answers to.

Every record carries the headings `Context`, `Decision`, `Alternatives`, `Consequences` and `Revisit trigger`, in that order. A record may add its own headings between them.

## The register

| ADR | Adjudicates | Decision |
|---|---|---|
| [0001](0001-local-first-gating.md) | none | Quality gates run locally first, through git hooks, with GitHub Actions as the backstop. |
| [0002](0002-storage-engine-sqlite.md) | D1 | Storage is one SQLite database file through `modernc.org/sqlite`, in WAL mode. |
| [0003](0003-protocol-http-json.md) | D2 | The protocol is HTTP/1.1 and JSON on the standard library, over a Unix socket by default, with long-poll pull. |
| [0004](0004-dlq-as-a-stream.md) | D3 | A dead-letter queue is a real stream, `<stream>.dlq`, and the copy mints a new message id. |
| [0005](0005-two-durability-modes.md) | D4 | There are two durability modes, group commit is always on, and an ack response is a durability promise under `full`. |
| [0006](0006-delivery-rows-ack-is-delete.md) | D5 | Delivery state is a durable row, an ack is a DELETE, and expiry is one indexed UPDATE in a 250 ms sweeper tick. |
| [0007](0007-attempt-counted-at-claim.md) | D6 | `attempts` increments at claim and is durable before the fetch response. |
| [0008](0008-fenced-plain-ack-tokens.md) | D7 | Ack tokens are plain, readable and fenced by attempt and generation, with no HMAC. |
| [0009](0009-flags-and-env-no-config-file.md) | D8 | Configuration is flags and `MESSQ_*` environment variables only, with no config file and no viper. |
| [0010](0010-cobra-without-viper.md) | D9 | The CLI is built on `spf13/cobra`, without viper and without scaffolding. |
| [0011](0011-ulid-identifiers.md) | D10 | Message ids are ULIDs. |
| [0012](0012-events-table-is-the-source-of-truth.md) | D11 | The in-transaction events table is the source of truth; logs and metrics are projections of one closed vocabulary. |
| [0013](0013-secure-by-default-minimalism.md) | D12 | Security is a Unix socket by default, bearer tokens for TCP, three roles, and nothing else at 1.0. |
| [0014](0014-testing-strategy.md) | D13 | Testing is a pure state machine, an independent reference model under rapid, a SIGKILL crash harness with a three-valued oracle, and no wall-clock sleeps. |
| [0015](0015-one-thin-go-client.md) | D14 | One thin Go client package, consumed by the CLI itself, with a `Worker` helper. |
| [0016](0016-phase-2-is-demand-ordered.md) | D15 | Scheduling, ordering and priority features are phase 2 and strictly demand-ordered; replication is not on the roadmap. |
| [0017](0017-module-path.md) | none | The module path is `github.com/a-holm/messq`. |

## Writing one

Copy `0000-template.md`, take the next free number, and keep the record short. The reasoning matters more than the prose. A new direct dependency needs a record of its own, per PLAN.md section 13.
