<!-- SPDX-License-Identifier: Apache-2.0 -->

# messq doctor — check reference

Every registered check documents itself here. Sections are generated from the
registry's own Summary and Explain strings and enforced by
`TestDocsDoctorHasSectionPerID`: a check that renames or forgets its
teaching paragraph fails the build instead of an incident review.

Findings carry this anchor in their `docs` field (`docs/doctor.md#<id>`), so an alert or cron log is always one click from
the paragraph that explains what fired and why it matters.

## backup.none_configured

**Summary:** no backup directory is configured for monitoring

messq cannot tell you when you last took a backup without somewhere to look. Point --backup-dir at the directory the timer writes to.

## backup.perms

**Summary:** backup payloads readable beyond their owner

Snapshots contain message bodies in full: group/other read on a backup file is the same leak as publishing them wider.

## backup.stale

**Summary:** newest known snapshot is older than --backup-max-age

Backups rot silently: this is the check that turns silent rot into a warn line cron emails somebody about.

## backup.unreadable

**Summary:** a snapshot on disk would not restore

quick_check fails or provenance keys are missing: whichever it is, this file will not restore. Take a fresh one and delete this politely.

## consumer.filter_matches_nothing

**Summary:** consumer filters match none of the stream's recent subjects

Publishes land on the stream but never on THIS consumer because its filter set selects nothing real. Usually a typo against the stream's actual subjects; the three most common ones ride along as evidence.

## consumer.flow_blocked

**Summary:** inflight pinned at max_ack_pending — workers are the bottleneck

Flow control is doing exactly what max_ack_pending is for, which here means: adding workers (or raising the cap) moves real throughput today.

## consumer.idle

**Summary:** a consumer with backlog stopped being delivered anything

Backlog with no deliveries is a stuck worker or an exhausted retry budget — either way messages are late RIGHT NOW. Is the worker running? messq lag and messq pending name the concrete rows.

## consumer.idle_no_backlog

**Summary:** nothing delivered for a long while, but there is no backlog either

The broker is healthy; the WORKER may be gone. If nobody re-created it that is usually the point at which you decide it was intentional and remove the consumer so lag views stop mourning it.

## consumer.max_deliver_unlimited

**Summary:** a consumer redelivers forever because max_deliver=0

max_deliver=0 disables the delivery ceiling: one poison message loops through the worker until someone notices. The §7 default is a small bound plus a DLQ, so terminal failures park instead of churning.

## consumer.max_deliver_unlimited_no_dlq

**Summary:** max_deliver=0 AND dead_policy=drop loses messages silently

With unlimited redelivery and drop-on-dead there is no ceiling and no parking lot; a message that kills the worker is retried forever and anything else that fails permanently vanishes. This combination is refused advice.

## consumer.oldest_pending

**Summary:** the oldest pending delivery is aging past useful

The §9.4 SLI: oldest_pending_age_seconds. At fifteen minutes a worker is visibly behind; past an hour every SLA conversation needs this number and an excuse.

## consumer.paused

**Summary:** this consumer has been paused for over an hour

Pausing is a human decision; a pause older than an hour is more often a forgotten experiment than a deliberate state. messq consumer resume undoes it in one line.

## consumer.stale_acks

**Summary:** acks are arriving after ack_wait (any nonzero rate alerts)

A stale ack means the worker held a message past its lease and the delivery already redelivered to someone else — every occurrence is a duplicate somebody absorbs. §9.4 says alert on any nonzero rate; doctor agrees.

## consumer.time_to_dead_exceeds_events

**Summary:** TimeToDead outruns event retention, so failures vanish before they can be diagnosed

When the full backoff ladder outlives --event-retention, the events that would explain a dead message are already trimmed when it dies (#11, #19). Raise retention above roughly ten TimeToDead or shorten the ladder.

## dlq.growing_undrained

**Summary:** DLQ grew in the window while nobody redrove anything

A poison message is the usual cause; DLQs nobody watches are the documented failure mode (#12). Redrive them deliberately after reading the causes, not after the disk reminds you.

## dlq.no_consumer

**Summary:** dead letters piled up with nobody watching that DLQ

Depth without a consumer means messages parked after failing and nothing observes them. Even a manual redrive routine beats silence.

## dlq.no_retention

**Summary:** a DLQ stream keeps everything forever

Unbounded dead-letter streams turn one bad consumer into an outage on layaway. Give the parking lot its own retention so redrives stay a choice and not an archaeology project.

## dlq.template_drift

**Summary:** an existing DLQ drifted from the current --dlq-* template

#12 creates DLQs from your current template settings; older ones keep the shape they were born with. Doctor reports the drift rather than mutating history.

## durability.fsync_implausible

**Summary:** an fdatasync faster than RAM suggests lying hardware

Real spinning disks and NAND cannot acknowledge an fdatasync in under ~25 µs; a median below that means a VM write-back cache, a 'nobarrier' mount or consumer SSD flush-simulating firmware is lying. The cost arrives during the power cut you took the setting for.

## durability.fsync_probe

**Summary:** measures fdatasync latency and projects durable msg/s

PLAN §12 promises re-baselined numbers nobody has to take on faith: doctor writes 4 KiB blocks plus fdatasync against the data dir and prints p50/p99/p99.9 together with the achievable commit rate at the observed batch size. --fsync-samples 0 turns the probe off.

## durability.mode_relaxed

**Summary:** --durability=relaxed trades crash safety for latency

Relaxed lets a crash between the OS page cache and disk lose recent publishes (D4). That can be a deliberate choice for a dev box; doctor names it loudly precisely so nobody inherits it unknowingly.

## durability.pragma

**Summary:** what the synchronous pragma actually read back

A publish that answered 201 survives power loss only when SQLite's synchronous pragma matches the durability mode. Doctor reads it back instead of trusting configuration. Offline it can only report its own connection — which is labeled as such, because that value cannot prove anything about the daemon's writer.

## durability.tmpfs

**Summary:** the data dir lives on tmpfs while durability demands otherwise

tmpfs is RAM: a reboot IS a power loss. Under --durability=full the durability promise is void no matter what pragmas say — move the data dir to a real filesystem.

## metrics.dropped_series

**Summary:** the metrics registry dropped series under pressure (#21)

Anything dropped stopped existing as far as dashboards are concerned. Raise --metrics-max-series or cut series-producing consumers before an incident makes you read the graph wrong.

## security.cleartext_public

**Summary:** a non-loopback listener serves cleartext HTTP today

Until native TLS lands (#40) termination belongs in front of messq: a reverse proxy or WireGuard tunnel, and never raw on a public interface.

## security.permissions

**Summary:** filesystem permissions around the data directory are wrong

#16's preflight rules are blunt on purpose: data dirs private, databases 0600, token files tighter than nothing at all. Doctor names the path and the chmod line.

## security.tcp_no_auth

**Summary:** a TCP listener has no auth file protecting it

Loopback TCP still shares the host with anything; without --auth-file any local process may publish. Mount an auth file or accept who knows.

## server.clock_jump

**Summary:** the wall clock regressed more than a second while running

Deadlines ride monotonic clocks but ULIDs do not — a backwards jump orders ids strangely and inflates TTL math anywhere callers used wall time.

## server.janitor_disabled

**Summary:** --janitor-interval 0 disables retention and trims (#27)

It is a test-only setting: retention promises, dedup trims and disk reserve all stop happening. Running production with zero janitor interval is choosing which of those hurts first.

## server.restart_loop

**Summary:** three or more daemon starts inside the last hour

A crash-looping broker hides partial availability behind restarts. The exit codes and journal tell you which component keeps dying first.

## server.restored

**Summary:** this data directory was restored from a backup snapshot

Everything published after the snapshot's read transaction began is gone, and deliveries INFLIGHT at snapshot time redeliver after the restore with their attempts intact — workers holding pre-restore tokens get stale_ack 409s named future_attempt. That is the documented safe direction.

## server.sweep_interval

**Summary:** --sweep-interval outruns the smallest ack_wait (#11)

The sweeper reclaims timed-out deliveries; if it wakes less often than some consumer's ack_wait, redelivery latency inherits that gap for every worker on that consumer.

## server.unclean_last_start

**Summary:** the most recent start followed an unclean shutdown

Expected after kill -9 during incident response; NOT expected after a systemctl stop. Repeated unclean stops mean lifecycle handling deserves a look (#17).

## server.unreachable

**Summary:** the daemon this run pointed at did not answer

Doctor diagnoses a running daemon over --addr. When nothing answers, every live-only check degrades to skip and THIS finding is the one that matters: either the daemon is down (start it) or --addr points somewhere else than the daemon listens on.

## server.version_skew

**Summary:** CLI version differs from the daemon's version

A skew between the driving binary and the serving binary means the two disagree about wire details by accident of build date. Upgrade whichever is behind; doctor reports rather than guesses which.

## storage.disk_headroom

**Summary:** free space versus minimum headroom and growth rate

Doctor computes days-to-full from the observed growth rate when it is known and always checks the absolute floor at four times --min-free-bytes. The ETA rides the evidence so cron logs become graphs for free.

## storage.events_share

**Summary:** the events table eating more than its share of the database

Events are the audit trail (§8), but past ~40% of database bytes they are retention policy failure wearing a tux. Raise --event-retention's teeth or lower the row cap; #27 trims for you once configured.

## storage.fs_type

**Summary:** names the filesystem under the data directory and refuses liars

flock on NFS/CIFS/FUSE/overlay is a cooperative fiction (#5): two processes can each believe they hold it. On those filesystems messq's data-directory locking cannot do its job — move the data dir to a local filesystem before trusting single-writer guarantees.

## storage.mount_options

**Summary:** nobarrier/barrier=0 void every durability claim on this mount

Mount options are where lying hardware hides in plain sight. A 'nobarrier' or 'barrier=0' tells the kernel writes need not hit stable storage before acking — exactly what durability=full assumed true.

## storage.readonly_latch

**Summary:** the daemon latched itself read-only after an fsyncgate event

After fdatasync stopped being trustworthy the daemon refused writes (D4) rather than lie about durability. This is a page, not a chore: clear the storage fault first, restart, and only then look at anything else.

## storage.reserve_unavailable

**Summary:** fallocate is unsupported here — disk headroom shrank (#27)

Without fallocate the reserved-tail protection cannot pin bytes, so --min-free-bytes becomes your only floor. Raise it accordingly.

## storage.wal_size

**Summary:** WAL size above budget, or a reader pinning checkpoints too long

A long-running read transaction (often a backup) pins the WAL tail: checkpoints stall and the file grows with write volume. This is a cost, not corruption — doctor names who is holding it.

## stream.no_consumers

**Summary:** a stream holds messages nobody consumes

Messages with no consumer are retained (and billed in bytes) while doing nothing. If the stream is deliberate, keep an eye on retention; otherwise remove it once its consumers are gone for good.

## stream.typo_suspect

**Summary:** a young, tiny, unconsumed stream looks like a typo of another stream

A publisher that mistypes a subject creates a new stream nobody reads; the messages are accepted (2xx!) but never delivered. The suspect profile: at most 10 messages, no consumers, under a week old, name within edit distance 2 of an existing stream.

