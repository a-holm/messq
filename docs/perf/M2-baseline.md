# M2 baseline — crash harness + verify + throughput gate

Recorded by issue #8 (crash harness v1). This file is the frozen methodology and the
reference numbers the later milestones (M3 tail latency, M4 real data plane) compare
against.

- commit: `8cb37dc81785`
- date: 2026-08-23
- Go: `go1.26.6 linux/amd64`

## Measurement environment (D1)

| | |
|---|---|
| CPU | Intel Core i7-3770K @ 3.50 GHz, 4C/8T |
| RAM | 32 GiB (32 811 984 kB) |
| kernel | 6.17.0-35-generic |
| bench dir | `/home/johan/bench-messq` |
| filesystem | **ecryptfs on NVMe** — `/home/johan` is ecryptfs (aes) over `/home/johan/.Private`, which is ext4 on `/dev/nvme0n1p4` (WD_BLACK AN1500) |

The D1 criterion ("≥ 5 000 durable 1 KiB msg/s on NVMe") is measured **on this machine's
owner-designated bench dir**, not on a bare NVMe. ecryptfs sits in the write path, so the
number below is an ecryptfs-on-NVMe number and understates a bare ext4 NVMe.

## Methodology (frozen)

`go test ./test/crash -run TestDurableThroughput -crash.gate -crash.gate.dir=/home/johan/bench-messq`

- 1 KiB deterministic payloads, open loop.
- `--durability=full` (synchronous=FULL), 32 publishers, 30 s steady state, one stream, no
  consumers.
- The measured quantity is **publish → durable response** — the D1 exit criterion itself,
  not raw insert rate.
- A raw `fsync` probe (4 KiB write + `fdatasync`, 1 000 samples) on the same filesystem is
  reported alongside, so the number is interpretable: achievable throughput ≈
  commit_batch_size / fsync_latency.

## Numbers

```
gate: 32 publishers, 1024-byte payloads, 30s steady state
gate: 10585 messages, 353 msg/s
gate: publish->durable p50=59.89ms p99=468.95ms p99.9=888.01ms
gate: fsync probe p50=67.28µs p99=3.76ms (1000 samples)
```

The fsync probe (p50 ≈ 67 µs) proves the storage is not the bottleneck: even a tiny
commit batch of 8 messages would clear 5 000 msg/s at that fsync cost. The publish→durable
p50 of ~60 ms is ~3 orders of magnitude above the fsync floor.

## D1 verdict

**MISS — 353 msg/s, but the measurement is not of the intended path.**

Root cause, confirmed by inspection and by a direct store benchmark (solo publish ≈
7.5 ms/msg even on tmpfs where fsync is a no-op): **the group-commit writer (#6) is not
wired into the `messq serve` binary.** `internal/cli/serve.go` and `internal/api` open the
store and publish through `Store.Publish`, but nothing calls `Store.NewWriter`, so every
publish runs `runSolo` — the engine-less fallback of one transaction, one commit, one fsync
per message. The commit-window / `--commit-max-batch` / `wal_autocheckpoint` / `cache_size`
tuning in the D1 revisit trigger cannot apply: there is no writer to tune.

Decision, per the D1 revisit trigger ("this issue does not fix it — it decides"):

1. **Defer the escape-hatch decision.** The 353 msg/s number is a wiring gap, not a
   storage-limit finding. The honest next step is to wire `Store.NewWriter` into `serve`
   (a `#7`-follow-up, tracked outside this issue — this issue does not modify
   `internal/cli` or `internal/api` behaviour), then re-measure this gate.
2. **The smoke floor stands.** The per-merge crash-smoke asserts `≥ 200 msg/s`, which is
   calibrated to catch a regression *back* to one-fsync-per-message; the current number
   (353 msg/s) clears it, so the smoke stays green while the writer remains unwired.
3. **The gate is the alarm.** `TestDurableThroughput -crash.gate` is now the mechanism that
   would have surfaced this on day one; CI does not assert the 5 000 number as evidence
   (per the brief, CI asserts only the smoke floor), but the number above is what the
   escape-hatch decision will be made on once the writer is wired.

## Crash-harness summary (M2)

The kill9 sweep over the real `messq serve` binary, reconciled against the three-valued
ledger oracle, is green at the smoke default (8 cycles):

```
full: ok=487 unknown=104 failed=0 present=6 absent=98 wal_tail=true
```

- zero acknowledged loss (`failed=0`),
- both survivor outcomes observed (`present=6`, `absent=98`) — the kill window is real,
- the WAL tail is non-empty at kill (`wal_tail=true`) — recovery does real work.

See `test/crash/crash_test.go` for the entry points and `internal/testutil/crash` for the
harness.
