# Fixture: the invariant register has no I7.

## S15. Invariant register

| ID | Statement | Predicate | Checked by | First required at |
|---|---|---|---|---|
| I1 | No acknowledged publish is lost. | ledger reconciliation | crash harness | #8 |
| I2 | Every pair is in exactly one state. | disjoint predicates | verify | #9 |
| I3 | An acked pair is never redelivered. | history predicate | rapid hook | #10 |
| I4 | attempts stays within max_deliver. | SQL predicate | verify | #11 |
| I5 | pending stays within max_ack_pending. | SQL aggregate | verify | #9 |
| I6 | cursor_seq is monotone. | comparison | rapid hook | #9 |
| I8 | Each pair dies at most once. | count over events | verify | #12 |
| I9 | Every duplicate has a named cause. | history predicate | rapid hook | #13 |
| I10 | Folding events reproduces state. | verify --deep | verify | #8 |
| I11 | Nothing is unbounded. | bounds register | review | #6 |
