# subjects

Subjects are NATS-syntax addresses. Tokens are dot-separated; `*` matches
exactly one token; `>` matches one or more trailing tokens.

| pattern | matches | does not match |
|---|---|---|
| `orders` | `orders` | `orders.created`, `orders.created.eu` |
| `orders.*` | `orders.created`, `orders.cancelled` | `orders.created.eu`, `orders` |
| `orders.>` | `orders.created`, `orders.created.eu` | `orders` |
| `>` | anything with at least one token | — |

The rules a worker leans on:

- An **exact grant** like `orders` never covers `orders.dlq` — a DLQ is a
  sibling subject, not a child. A trailing `orders*` (prefix star, no dot)
  covers the family including `orders.dlq`.
- `*` and `>` are only wildcards as WHOLE tokens: `ord*` is a literal token
  that matches nothing but `ord*`.
- A stream's `subjects` are the patterns it accepts; a consumer's `filters`
  are the patterns it wants. A delivery needs both to agree with the message's
  subject.

Examples with the CLI:

```
messq stream add orders --subjects 'orders.>'
messq consumer add orders billing --filter 'orders.created'
messq peek orders --subject 'orders.created' --last 5
```

Publishing to a wildcard (`messq pub orders 'orders.>'`) is refused: `>` is a
pattern, not an address. Completion suggests the literal prefix (`orders.`)
with `NoSpace` for exactly this reason.
