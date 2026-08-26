-- SPDX-License-Identifier: Apache-2.0
-- Migration 0004: retention watermark columns on the stream counters (issue #27 §4,
-- Decision 2). An ALTER against #7's frozen 0002 artefact — never a re-CREATE — so an
-- existing data directory keeps its counts and the writer's three-column INSERT keeps
-- working through the defaults.
--
--   expired_seq  highest seq removed by retention from this stream (oldest-first
--                deletion makes one watermark exact). peek/trace answer "seq N was
--                removed by retention" from it (#28 consumes it).
--   expired_at   when, unix ms.
ALTER TABLE stream_stats ADD COLUMN expired_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE stream_stats ADD COLUMN expired_at INTEGER NOT NULL DEFAULT 0;
