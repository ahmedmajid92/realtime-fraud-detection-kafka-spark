-- Schema for the fraud detection stack (runs once, on first start of an empty volume).

-- Every scored transaction, written by Spark (idempotent upsert on transaction_id).
CREATE TABLE IF NOT EXISTS predictions (
    transaction_id     UUID PRIMARY KEY,
    event_time         TIMESTAMPTZ      NOT NULL,
    processed_at       TIMESTAMPTZ      NOT NULL,
    stored_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    source             TEXT             NOT NULL CHECK (source IN ('replay', 'manual', 'e2e')),
    replay_pass        INTEGER          NOT NULL,
    amount             DOUBLE PRECISION NOT NULL,
    hour               SMALLINT         NOT NULL,
    fraud_probability  DOUBLE PRECISION NOT NULL,
    prediction         SMALLINT         NOT NULL CHECK (prediction IN (0, 1)),
    threshold          DOUBLE PRECISION NOT NULL,
    actual_class       SMALLINT                  CHECK (actual_class IN (0, 1)),
    model_name         TEXT             NOT NULL,
    latency_ms         DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_predictions_processed_at
    ON predictions (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_source_processed_at
    ON predictions (source, processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_alerts
    ON predictions (processed_at DESC) WHERE prediction = 1;

-- Shared runtime settings (alert threshold written by the dashboard, read by Spark).
CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Spark Structured Streaming progress events (System Health page; pruned to ~1 hour).
CREATE TABLE IF NOT EXISTS stream_progress (
    id                         BIGSERIAL PRIMARY KEY,
    query_id                   TEXT,
    run_id                     TEXT,
    batch_id                   BIGINT      NOT NULL,
    recorded_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    num_input_rows             BIGINT,
    input_rows_per_second      DOUBLE PRECISION,
    processed_rows_per_second  DOUBLE PRECISION,
    batch_duration_ms          BIGINT,
    end_offsets                JSONB
);

CREATE INDEX IF NOT EXISTS idx_stream_progress_recorded_at
    ON stream_progress (recorded_at DESC);

CREATE OR REPLACE VIEW v_fraud_alerts AS
    SELECT * FROM predictions WHERE prediction = 1;
