CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events_raw
(
  event_id String,
  timestamp DateTime,
  source String,
  source_id String,
  event_type String,
  video_id String,
  playback_pos Float64,
  session_id String,
  meta String,
  err_score Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_type, timestamp);