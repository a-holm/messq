<!-- SPDX-License-Identifier: Apache-2.0 -->

# Backup and restore (issue #30)

**Restore is stop + copy + start. That is the whole story, in one sentence.**

## Taking a backup

```console
messq backup /var/backups/messq/$(date -u +%FT%H%MZ).db --data-dir /var/lib/messq
```

`messq backup` runs SQLite's `VACUUM INTO` on a dedicated read-only connection.
The snapshot is consistent while the daemon runs **and** after it stops —
nothing about the pipeline needs the daemon down, so an incident never has to
choose between a snapshot and diagnosis.

What the command guarantees, in order:

1. Every refusal happens **before a page is copied**: destination exists
   without `--force` (exit 4), destination inside the data directory (exit 2),
   destination directory not writable (exit 7), free space below estimate × 1.1
   (exit 1).
2. The write goes to a private `0700` temp directory next to the destination,
   then is renamed into place only after the self-check passes — a failing or
   killed backup leaves the destination either absent or untouched, never
   partial.
3. The finished file reopens read-only for its own verification:
   `quick_check` (`integrity_check` under `--verify full`), matching schema
   and user versions, preserved `page_size`/`auto_vacuum`, per-stream head
   counts.
4. Provenance stamps into the file itself (`snapshot_taken_at`,
   `_source_node`, `_source_path`, `_tool_version`, `_source_live`,
   `_stream_heads`) plus `clean_shutdown=1`, so the copy explains itself to
   whoever restores it without reading this document.

### What a backup costs

The read transaction pins the WAL tail while it runs: on a busy broker the
10-minute backup of a hot system grows `messq.db-wal` by roughly the write
volume for those minutes. That is a cost, not a bug — the docs' advice is
off-peak scheduling (and `ionice -c3`) with the timer below.

## Restoring

Stop the daemon, replace the data-directory's `messq.db`, start the daemon:

```console
systemctl stop messq
cp /var/backups/messq/20261104T1200Z.db /var/lib/messq/messq.db
chown messq:messq /var/lib/messq/messq.db && chmod 0600 /var/lib/messq/messq.db
systemctl start messq
```

On first open the store recognises the `snapshot_*` rows, converts them into
permanent `restored_*` provenance, deletes the snapshot keys, and emits exactly
one `admin.action` row announcing the restore. Everything published after the
snapshot's read transaction began is gone; deliveries that were INFLIGHT at
snapshot time redeliver afterwards with their attempt counts intact, which
shows up as duplicates whose cause is visible via `messq trace`
(`recovery.reclaimed`). Workers holding pre-restore tokens get `409 stale_ack`
(`future_attempt`) — the safe direction.

Never run two daemons from one lineage: both directories carry the same
`node_id`, and `flock` protects only within a directory.

## Not a backup

Copying files while the daemon writes them is not a backup: you get the main
file without its WAL (`-wal` holds acknowledged-but-not-yet-checkpointed
publishes) or torn pages. There is no `-wal` to copy safely, no `messq restore`
verb wrapping install(1), and no remote targets — by design; see PLAN §4.5 and
docs/non-goals.md once #35 lands the wording there.

## The timer

`contrib/systemd/messq-backup.{service,timer}` package the off-peak schedule
the cost paragraph above recommends. Point doctor at the same directory to get
freshness as a finding instead of hope:

```console
messq doctor --backup-dir /var/backups/messq
```
