# 0009. Configure with flags and MESSQ_ environment variables only

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: D8
- Relates to: PLAN.md section 2 D8, PLAN.md section 8, issue #4, issue #17

## Context

A configuration file looks free and is not. It brings a parser, a search path, a precedence order between file, environment and flags, and a permanent support category called "which setting won?". It also freezes the flag surface early, because a file format is harder to change than a flag.

messq's success criterion is under sixty seconds from download to first acked message with no configuration file. The daemon has to be usable with defaults and a data directory.

## Decision

For v1, configuration is command-line flags and `MESSQ_*` environment variables. There is no configuration file. Flags win over environment variables, and there is no third source, so precedence is one sentence.

Runtime-mutable settings, meaning the log level and the auth token file, reload on SIGHUP. Everything else requires a restart.

viper is never a dependency. That was unanimous across every plan.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| A TOML or YAML configuration file from day one | It is what operators expect, and it makes a complex deployment reproducible. | It freezes a flag surface that has not stabilised, and it adds a precedence bug class before there is anything to configure. It stays available after 1.0 if it is demanded, and by then the flag names will have stopped moving. |
| viper | It gives files, environment, remote configuration and live reload in one dependency. | It brings a large dependency tree and an opaque precedence model for a daemon with a small flag surface. Every plan rejected it independently. |
| Environment variables only, in the twelve-factor style | It is container-native and needs no flag parsing. | It makes the CLI unusable interactively, and the same binary is both the daemon and the client. |
| A file plus flags with documented precedence | It is the conventional answer and it works. | The documentation cost is real and permanent: every support question about an unexpected setting becomes an investigation of three sources. Deferring the file is free; removing it later is not. |

## Consequences

There is one place a setting can come from, so `messq doctor` can print the live value and its source in one line without qualification.

A complex deployment writes its flags into a systemd unit or a container command line, which is a place that is already version-controlled.

The cost lands on operators with many settings: a long command line instead of a file. The shipped systemd unit and the packaging carry sensible flags, so the common case is still short.

A configuration file after 1.0 is a compatible addition, because flags will still win.

## Revisit trigger

The flag surface stabilising, plus real demand: three independent requests for a configuration file after 1.0. The format would be TOML, files would lose to environment variables, which would lose to flags, and that order would be documented before the parser is written.
