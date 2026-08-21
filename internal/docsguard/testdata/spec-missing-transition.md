# Fixture: the transition table has no T2.

### S6.1 The table

| ID | From | Trigger | Guard | Effect | Event | On guard failure |
|---|---|---|---|---|---|---|
| T1 | UNSEEN | fetch top-up | filter matches | insert a READY row | none | held |
| T3 | INFLIGHT | ack | fenced | delete the row | `msg.ack` | stale |
