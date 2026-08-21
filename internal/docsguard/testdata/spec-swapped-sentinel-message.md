# Fixture: two sentinels carry each other's messages.

## S13. Error outcomes

| Sentinel | Message | Raised by |
|---|---|---|
| `errs.ErrNotFound` | already exists | A named stream, consumer or message that does not exist, including a settle whose consumer was deleted (S3.3 step 2). |
| `errs.ErrConflict` | not found | A create that collides with an existing name, and a redrive past the redrive-count guard (T11). |
| `errs.ErrBadRequest` | invalid request | A malformed or out-of-range argument: a `delay_ms` outside `[0, 86400000]` (S8.3), an empty `backoff_ms` (S8.2), an extend past `max_ack_wait` (T7), a user header inside the reserved prefix (S3.4), a user stream name ending in `.dlq` (S3.4), an unconfirmed destructive action (T10). |
| `errs.ErrBadSubject` | subject is not valid or not accepted by this stream | A subject or pattern the grammar rejects, or one the target stream's patterns do not accept. |
| `errs.ErrTooLarge` | message exceeds max_msg_size | A body above the stream's `max_msg_size`. |
| `errs.ErrStreamFull` | stream is at its limit and discard=new | A publish into a stream at its limit with `discard = new`. |
| `errs.ErrFlowControl` | max_ack_pending reached | A fetch whose top-up is held by the `pending(c)` bound (T1). |
| `errs.ErrStaleAck` | stale ack: the message was already redelivered | A settle whose token attempt does not match the live row (T3b). |
| `errs.ErrUnknownToken` | unknown or malformed ack token | A token that does not parse, is over 171 bytes, or names nothing (S3.3 step 1). |
| `errs.ErrWrongGen` | token generation is stale; the consumer was reset | A settle whose token predates a `seek` or a `purge` (S3.3 step 3). |
| `errs.ErrPaused` | consumer is paused | A fetch against a paused consumer (T1). |
| `errs.ErrDiskFull` | insufficient free disk space | A publish below `--min-free-bytes` (S11.3). Settles are not rejected for this reason. |
| `errs.ErrReadOnly` | storage is latched read-only | Any write after a commit fault latched the process (S11.3). |
| `errs.ErrShuttingDown` | shutting down | A request that arrived during the graceful drain (S12). |
| `errs.ErrUnauthorized` | authentication required | A request with no credentials where credentials are required. |
| `errs.ErrForbidden` | not permitted for this token | A request whose token role or stream scope does not cover the operation. |
| `errs.ErrLocked` | data directory is locked by another process | A second `messq serve` against a data directory held under `flock`. |
| `errs.ErrSchemaNewer` | data directory schema is newer than this binary | A data directory written by a newer binary (S12 step 2). |
| `errs.ErrUnavailable` | daemon unreachable | A client-side failure to reach the daemon. It is never produced by the daemon. |
