-- SPDX-License-Identifier: Apache-2.0
-- Migration 0002: stream counters (issue #7 §5). msgs/bytes are maintained by the
-- writer inside every insert/delete transaction, so stream info stays constant-time
-- with respect to stream size; the delete cascade keeps the table orphan-free.
CREATE TABLE stream_stats (
  stream TEXT PRIMARY KEY REFERENCES streams(name) ON DELETE CASCADE,
  msgs   INTEGER NOT NULL,
  bytes  INTEGER NOT NULL
) STRICT;
