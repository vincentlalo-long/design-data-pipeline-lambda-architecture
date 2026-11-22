-- Serving layer schema for Lambda Architecture (PostgreSQL 16)
-- Invariant: Result = BatchAgg(seq <= H) + SpeedEvents(seq > H)

CREATE TABLE IF NOT EXISTS pipeline_state (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    batch_watermark_seq BIGINT NOT NULL DEFAULT 0,
    batch_runs INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO pipeline_state (id, batch_watermark_seq, batch_runs)
VALUES (1, 0, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS batch_category_agg (
    category TEXT PRIMARY KEY,
    event_count BIGINT NOT NULL,
    total_amount_cents BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS speed_events (
    event_id TEXT PRIMARY KEY,
    seq BIGINT NOT NULL,
    category TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_speed_events_seq ON speed_events (seq);
CREATE INDEX IF NOT EXISTS idx_speed_events_category ON speed_events (category);

-- Unified Lambda serving view combining batch snapshot (seq <= H) and speed events (seq > H)
CREATE OR REPLACE VIEW serving_category_agg AS
WITH wm AS (
    SELECT batch_watermark_seq AS h FROM pipeline_state WHERE id = 1
),
combined AS (
    SELECT category, event_count, total_amount_cents
    FROM batch_category_agg
    UNION ALL
    SELECT s.category, COUNT(*)::BIGINT AS event_count, COALESCE(SUM(s.amount_cents), 0)::BIGINT AS total_amount_cents
    FROM speed_events s, wm
    WHERE s.seq > wm.h
    GROUP BY s.category
)
SELECT
    category,
    SUM(event_count)::BIGINT AS event_count,
    SUM(total_amount_cents)::BIGINT AS total_amount_cents
FROM combined
GROUP BY category;
