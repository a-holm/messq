# durability

messq has two durability modes; `messq doctor` tells you which one your daemon
is actually running (`check yours: messq doctor`).

**full** (the default) maps to SQLite's `synchronous=FULL` in WAL mode: every
commit fsyncs the WAL before the write is acknowledged. A `201 Created` from a
publish means the message survived the power cut that follows. This is the
mode `messq quickstart` runs, and the mode an operator never has to think
about.

**relaxed** maps to `synchronous=NORMAL`: commits are acknowledged when the
WAL write is buffered, and the fsync happens on a checkpoint. It trades crash
safety for latency; the doctor flags a relaxed daemon loudly because the trade
is yours to justify, not the daemon's to hide silently.

Group commit makes full affordable: one fsync covers every publish waiting in
the commit window, so durable throughput is not 1/fsync. The writer engine is
the production path; a store without it still serves via the solo runner.

Fsync failure is a latch, not a log line: when the fsync gate fires the daemon
refuses further writes (health shows the degradation), keeps answering reads
for `--fatal-drain`, and exits 74. Never restart over a latched daemon without
`messq verify --deep` — the invariant checker is the arbiter of what the disk
actually kept.
