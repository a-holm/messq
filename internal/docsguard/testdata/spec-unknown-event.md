# Fixture: a transition emits an event name that is not in the closed vocabulary.

### S6.1 The table

| ID | From | Trigger | Guard | Effect | Event | On guard failure |
|---|---|---|---|---|---|---|
| T1 | UNSEEN | fetch top-up | filter matches | insert a READY row | none | held |
| T2 | READY | fetch claim | now past visible_at | claim the row | `msg.acked` | the row is skipped |
