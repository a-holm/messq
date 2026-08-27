# SPDX-License-Identifier: Apache-2.0

# Child-side exit codes (`messq sub --exec`, issue #25 §3)

With `--exec`, the child's exit code IS the ack decision. The table below is the
generated source for the one-time hint and normative for every transport that
reads a reason.

| Code | Transition | Reason recorded | Meaning |
|---|---|---|---|
| 0    | ack | — | done; stderr is mirrored, never stored |
| 75   | nak (consumer backoff) | captured stderr | EX_TEMPFAIL — retry later |
| 65   | term — straight to `<stream>.dlq` | captured stderr | EX_DATAERR: this PAYLOAD is unprocessable, one attempt only |
| else | nak (consumer backoff) | `exit N` + captured stderr | ordinary failure; reported as unexpected |

A child killed by a signal S naks with reason `killed by SIG<NAME>` plus
captured stderr. With `--exec-shell`, a signalled grandchild surfaces as a plain
`exit N` (no `128+N` special casing) — the mapping is defined on the DIRECT child
only, and `else` treats it like any non-zero exit.

The rest of the sysexits block is deliberately NOT special-cased: terminating on
`77` (EX_NOPERM) or `78` (EX_CONFIG)` would dead-letter healthy payloads over
operator misconfiguration — the messages are fine, so everything but `65`
retries.

Runtime spawn failures (EAGAIN, ENOMEM, missing binary after startup validation)
nak with a 5 s retry-after and reason `could not start <argv>: <cause>`, plus
`--exec-max-spawn-failures` stops the worker after five consecutive failures.

The child's stderr becomes the failure reason: inspect it with
`messq trace <msg-id>`; a DLQ'd message carries it in `Messq-Last-Reason`.

    hint: with --exec, the child's exit code is the ack decision:
            0    ack    done
            75   nak    EX_TEMPFAIL — retry with the consumer's backoff  [1s 5s 30s 2m 10m]
            65   term   EX_DATAERR  — permanent, straight to <stream>.dlq
            else nak    treated like 75 and reported as an unexpected failure
          the child's stderr becomes the failure reason: messq trace <msg-id>
          this hint prints once per run.  messq help exit-codes

(The block above is what `--exec` prints once per run on the first non-zero
child exit, suppressed by `--no-hints`; see issue #25 §9.)
