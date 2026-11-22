-- Q1: COUNT and SUM(amount_cents) by category
SELECT
    category,
    event_count,
    total_amount_cents
FROM serving_category_agg
ORDER BY total_amount_cents DESC, category ASC;

-- Q2: System-wide total COUNT and SUM(amount_cents)
SELECT
    COALESCE(SUM(event_count), 0)::BIGINT AS total_count,
    COALESCE(SUM(total_amount_cents), 0)::BIGINT AS total_amount_cents
FROM serving_category_agg;

-- Diagnostic: Inspect current watermark and branch contributions
SELECT
    ps.batch_watermark_seq AS watermark_h,
    ps.batch_runs,
    (SELECT COALESCE(SUM(event_count), 0) FROM batch_category_agg) AS batch_events,
    (SELECT COUNT(*) FROM speed_events WHERE seq > ps.batch_watermark_seq) AS active_speed_events,
    (SELECT COUNT(*) FROM speed_events WHERE seq <= ps.batch_watermark_seq) AS stale_speed_events
FROM pipeline_state ps
WHERE ps.id = 1;
