# scripting

messq is built to be driven. The rules:

**Output modes.** `--output table` for humans, `--output json` for one
document, `--output ndjson` for a stream of records. Never a fourth mode. On a
pipe, `auto` resolves to json; on a terminal, to table. In scripts, always pin
the mode explicitly.

**Data and narration never mix.** Data goes to stdout; warnings, progress and
hints go to stderr. `messq lag --output json | jq` is safe by construction.

**jq recipes.**

```
messq version --output json | jq .commit
messq auth ls --auth-file /etc/messq/tokens --output json | jq '.tokens[].id'
```

**The five-line curl worker.** Anything the CLI can do, the API can do:

```
ADDR=unix:///run/messq/messq.sock
curl --unix-socket /run/messq/messq.sock \
  -X POST localhost/v1/streams/orders/consumers/worker/fetch \
  -d '{"wait_ms":5000}' | jq -r '.messages[].id'
```

**Exit codes.** Every command exits 0–7 (see `messq help exit-codes`). `--exec`
workers additionally speak the sysexits contract (issue #25):

| exit | meaning |
|---|---|
| 0 | ack: the message settled |
| 65 | term: poison payload, straight to the DLQ (only 65 terminates) |
| 75 | nak: temporary failure (EX_TEMPFAIL), retry with backoff |
| other | nak with the exit code as the reason — misconfiguration never dead-letters fine payloads |

**Environment.** Flags and `MESSQ_*` variables are the only configuration —
no config file exists (D8). `NO_COLOR=1` disables ANSI everywhere; output is
colour-free on pipes regardless.
