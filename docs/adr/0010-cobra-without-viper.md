# 0010. Build the CLI on cobra, without viper

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D9
- Relates to: PLAN.md section 2 D9, PLAN.md section 8, PLAN.md section 13, issue #4, issue #23

## Context

The CLI is the product surface: every daemon capability is a subcommand, and the widest funnel is `messq sub --exec`, which turns a shell script into a durable worker. The dependency budget is eight direct non-test modules, so a CLI framework has to earn a whole row.

Nine of the eleven plans chose cobra. The go-purist plan dissented and proposed a hand-rolled dispatcher, on the grounds that argument parsing is not hard and a dependency is forever.

What decides it is not parsing. It is dynamic shell completion of live stream and consumer names: an operator typing a stream name at three in the morning types it wrong, and completion that queries the running daemon is a reliability feature, not a convenience.

## Decision

The CLI is built on `spf13/cobra` with `pflag`. `RunE` everywhere, `SilenceUsage` set so an operational error does not print a usage wall, persistent `--addr`, `--output` and `--token-file` flags, dynamic completion of live stream and consumer names with a 200 ms budget and silent failure, and generated man pages from the command tree.

There is no viper and no `cobra-cli` scaffolding. The command tree is written by hand.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| A hand-rolled dispatcher on `flag` | It saves a dependency and every line of it is readable. The go-purist plan is right that the parsing itself is easy. | It forfeits dynamic completion, generated man pages and consistent help output, and each of those would then be hand-built. The dependency is one row of eight, and it buys three features that would otherwise be three projects. |
| urfave/cli | Comparable feature set and a smaller API. | Weaker completion support, and cobra is what the Go operations ecosystem already generates completions for. |
| cobra plus viper | The default pairing, and it would give configuration files for free. | Decision D8 says there is no configuration file. viper was rejected unanimously and independently. |
| `cobra-cli` scaffolding | It writes the boilerplate. | It writes boilerplate this project would then have to read and maintain, including a viper wiring it does not want. |

## Consequences

Completion queries the daemon, so it must never hang: the 200 ms budget and silent failure are part of the contract, and a completion that times out prints nothing rather than an error.

Man pages and the documented command reference are generated from the command tree, so the CLI documentation cannot drift from the CLI.

The cost is one dependency row and cobra's own transitive tree, which is `pflag` and nothing else that matters. `scripts/dep-budget.sh` counts cobra and pflag as one budget row, matching PLAN.md section 13.

The project has to keep `RunE` discipline: no command calls `os.Exit`, every command returns an error, and the run pattern maps errors onto the documented exit codes. A lint rule enforces the `os.Exit` half.

## Revisit trigger

None. Removing cobra after the completion and man-page surfaces exist would be a user-visible regression.
