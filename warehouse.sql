-- =============================================================
-- Revenue Intelligence & Churn Decision Platform
-- SQL Architecture: RAW → CLEAN → ANALYTICS → BUSINESS
-- Engine: SQLite (structured like Snowflake/BigQuery)
-- Author: Revenue Intelligence Platform v1.0
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- LAYER 1: WAREHOUSE — CLEAN TABLES
-- Loaded from CSV by pipeline. Schema-enforced.
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users_clean (
    user_id         TEXT PRIMARY KEY,
    email           TEXT,
    name            TEXT,
    country         TEXT,
    plan            TEXT,
    channel         TEXT,
    age             INTEGER,
    created_at      TIMESTAMP,
    last_login_at   TIMESTAMP,
    is_active       INTEGER,     -- 0/1
    lifetime_value  REAL
);

CREATE TABLE IF NOT EXISTS orders_clean (
    order_id        TEXT PRIMARY KEY,
    user_id         TEXT REFERENCES users_clean(user_id),
    category        TEXT,
    status          TEXT,
    amount          REAL,
    net_amount      REAL,
    quantity        INTEGER,
    discount_pct    REAL,
    created_at      TIMESTAMP,
    delivered_at    TIMESTAMP,
    payment_method  TEXT
);

CREATE TABLE IF NOT EXISTS events_clean (
    event_id        TEXT PRIMARY KEY,
    user_id         TEXT REFERENCES users_clean(user_id),
    event_type      TEXT,
    session_id      TEXT,
    platform        TEXT,
    page            TEXT,
    event_ts        TIMESTAMP,
    duration_sec    REAL
);


-- ─────────────────────────────────────────────────────────────
-- LAYER 2: ANALYTICS — USER FEATURES (RFM + Activity)
-- Business logic: Recency, Frequency, Monetary, Events
-- ─────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS user_features;

CREATE TABLE user_features AS

WITH

-- Observation window anchor: latest order date in dataset
anchor AS (
    SELECT DATE(MAX(created_at)) AS snapshot_date
    FROM   orders_clean
    WHERE  status = 'completed'
),

-- RFM base from completed orders only
order_agg AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT o.order_id)                          AS frequency,
        SUM(o.net_amount)                                   AS total_revenue,
        AVG(o.net_amount)                                   AS avg_order_value,
        MAX(DATE(o.created_at))                             AS last_order_date,
        MIN(DATE(o.created_at))                             AS first_order_date,
        -- Recency: days since last order relative to snapshot
        CAST(
            JULIANDAY((SELECT snapshot_date FROM anchor)) -
            JULIANDAY(MAX(DATE(o.created_at)))
        AS INTEGER)                                         AS recency_days,
        -- Avg inter-order gap (loyalty signal)
        CASE
            WHEN COUNT(DISTINCT o.order_id) > 1
            THEN CAST(
                (JULIANDAY(MAX(DATE(o.created_at))) - JULIANDAY(MIN(DATE(o.created_at))))
                / (COUNT(DISTINCT o.order_id) - 1)
            AS REAL)
            ELSE NULL
        END                                                 AS avg_order_gap_days,
        SUM(CASE WHEN o.status = 'returned'   THEN 1 ELSE 0 END) AS return_count,
        SUM(CASE WHEN o.status = 'cancelled'  THEN 1 ELSE 0 END) AS cancel_count
    FROM   orders_clean o
    JOIN   users_clean u ON o.user_id = u.user_id
    WHERE  o.status IN ('completed','returned','cancelled')
    GROUP  BY o.user_id
),

-- Event activity per user
event_agg AS (
    SELECT
        user_id,
        COUNT(*)                                             AS total_events,
        COUNT(DISTINCT session_id)                           AS total_sessions,
        SUM(duration_sec)                                    AS total_time_sec,
        SUM(CASE WHEN event_type='purchase'       THEN 1 ELSE 0 END) AS purchase_events,
        SUM(CASE WHEN event_type='add_to_cart'    THEN 1 ELSE 0 END) AS cart_events,
        SUM(CASE WHEN event_type='page_view'      THEN 1 ELSE 0 END) AS pageview_events,
        MAX(DATE(event_ts))                                  AS last_event_date,
        CAST(
            JULIANDAY((SELECT snapshot_date FROM anchor)) -
            JULIANDAY(MAX(DATE(event_ts)))
        AS INTEGER)                                          AS days_since_event
    FROM   events_clean
    GROUP  BY user_id
),

-- Cohort: acquisition month
user_cohort AS (
    SELECT
        user_id,
        STRFTIME('%Y-%m', created_at) AS cohort_month,
        plan,
        country,
        channel,
        is_active,
        lifetime_value              AS reported_ltv
    FROM users_clean
)

SELECT
    uc.user_id,
    uc.cohort_month,
    uc.plan,
    uc.country,
    uc.channel,
    uc.is_active,
    uc.reported_ltv,

    -- RFM features (NULL-safe with COALESCE)
    COALESCE(oa.recency_days, 9999)         AS recency_days,
    COALESCE(oa.frequency, 0)               AS frequency,
    COALESCE(oa.total_revenue, 0)           AS total_revenue,
    COALESCE(oa.avg_order_value, 0)         AS avg_order_value,
    oa.last_order_date,
    oa.first_order_date,
    COALESCE(oa.avg_order_gap_days, 9999)   AS avg_order_gap_days,
    COALESCE(oa.return_count, 0)            AS return_count,
    COALESCE(oa.cancel_count, 0)            AS cancel_count,

    -- Engagement features
    COALESCE(ea.total_events, 0)            AS total_events,
    COALESCE(ea.total_sessions, 0)          AS total_sessions,
    COALESCE(ea.total_time_sec, 0)          AS total_time_sec,
    COALESCE(ea.purchase_events, 0)         AS purchase_events,
    COALESCE(ea.cart_events, 0)             AS cart_events,
    COALESCE(ea.pageview_events, 0)         AS pageview_events,
    ea.last_event_date,
    COALESCE(ea.days_since_event, 9999)     AS days_since_event

FROM user_cohort uc
LEFT JOIN order_agg  oa ON uc.user_id = oa.user_id
LEFT JOIN event_agg  ea ON uc.user_id = ea.user_id;

-- Index for query performance
CREATE INDEX IF NOT EXISTS idx_uf_userid ON user_features(user_id);
CREATE INDEX IF NOT EXISTS idx_uf_revenue ON user_features(total_revenue DESC);


-- ─────────────────────────────────────────────────────────────
-- LAYER 3: CHURN SCORING — EXPLAINABLE WEIGHTED MODEL
-- Weights: Recency(40%) + Frequency(25%) + Monetary(20%) + Activity(15%)
-- Output: churn_score 0-100, segment label
-- ─────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS churn_scores;

CREATE TABLE churn_scores AS

WITH

-- Percentile bounds for normalization
stats AS (
    SELECT
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY recency_days)    AS p95_recency,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY frequency)       AS p95_frequency,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_revenue)   AS p95_revenue,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_events)    AS p95_events
    FROM user_features
    WHERE recency_days < 9999  -- exclude users with no orders
),

-- Normalized sub-scores (0 = best, 100 = worst / churned)
normalized AS (
    SELECT
        uf.user_id,
        uf.plan,
        uf.country,
        uf.total_revenue,
        uf.recency_days,
        uf.frequency,
        uf.avg_order_value,
        uf.total_events,
        uf.days_since_event,

        -- RECENCY SCORE: higher recency = higher churn risk (0=recent, 100=long ago)
        ROUND(
            MIN(100.0, (uf.recency_days / NULLIF(s.p95_recency, 0)) * 100)
        , 2) AS recency_score,

        -- FREQUENCY SCORE: lower frequency = higher churn risk (inverted)
        ROUND(
            MAX(0.0, 100.0 - MIN(100.0, (uf.frequency / NULLIF(s.p95_frequency, 0)) * 100))
        , 2) AS frequency_score,

        -- MONETARY SCORE: lower revenue = higher churn risk (inverted)
        ROUND(
            MAX(0.0, 100.0 - MIN(100.0, (uf.total_revenue / NULLIF(s.p95_revenue, 0)) * 100))
        , 2) AS monetary_score,

        -- ACTIVITY SCORE: lower event count = higher churn risk (inverted)
        ROUND(
            MAX(0.0, 100.0 - MIN(100.0, (uf.total_events / NULLIF(s.p95_events, 0)) * 100))
        , 2) AS activity_score

    FROM user_features uf
    CROSS JOIN stats s
    WHERE uf.recency_days < 9999
)

SELECT
    user_id,
    plan,
    country,
    total_revenue,
    recency_days,
    frequency,
    avg_order_value,
    total_events,
    recency_score,
    frequency_score,
    monetary_score,
    activity_score,

    -- COMPOSITE CHURN SCORE (weighted)
    ROUND(
        (recency_score  * 0.40) +
        (frequency_score* 0.25) +
        (monetary_score * 0.20) +
        (activity_score * 0.15)
    , 2) AS churn_score,

    -- SEGMENT CLASSIFICATION
    CASE
        WHEN ROUND(
                (recency_score * 0.40) + (frequency_score * 0.25) +
                (monetary_score * 0.20) + (activity_score * 0.15), 2
             ) >= 80 THEN 'Churned'
        WHEN ROUND(
                (recency_score * 0.40) + (frequency_score * 0.25) +
                (monetary_score * 0.20) + (activity_score * 0.15), 2
             ) >= 60 THEN 'High Risk'
        WHEN ROUND(
                (recency_score * 0.40) + (frequency_score * 0.25) +
                (monetary_score * 0.20) + (activity_score * 0.15), 2
             ) >= 35 THEN 'At Risk'
        ELSE 'Active'
    END AS segment

FROM normalized;

CREATE INDEX IF NOT EXISTS idx_cs_score ON churn_scores(churn_score DESC);
CREATE INDEX IF NOT EXISTS idx_cs_segment ON churn_scores(segment);


-- ─────────────────────────────────────────────────────────────
-- LAYER 4: REVENUE INTELLIGENCE — AT-RISK QUANTIFICATION
-- revenue_at_risk = total_revenue * churn_score / 100
-- potential_recovery = revenue_at_risk * 0.25
-- ─────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS revenue_at_risk;

CREATE TABLE revenue_at_risk AS

WITH ranked AS (
    SELECT
        cs.user_id,
        cs.plan,
        cs.country,
        cs.segment,
        cs.churn_score,
        cs.total_revenue,
        cs.recency_days,
        cs.frequency,
        cs.avg_order_value,
        cs.total_events,

        -- Core revenue intelligence metrics
        ROUND(cs.total_revenue * cs.churn_score / 100.0, 2)          AS revenue_at_risk,
        ROUND(cs.total_revenue * cs.churn_score / 100.0 * 0.25, 2)   AS potential_recovery_value,

        -- Priority rank within segment
        ROW_NUMBER() OVER (
            PARTITION BY cs.segment
            ORDER BY (cs.total_revenue * cs.churn_score / 100.0) DESC
        ) AS priority_rank_in_segment,

        -- Global priority rank by revenue at risk
        ROW_NUMBER() OVER (
            ORDER BY (cs.total_revenue * cs.churn_score / 100.0) DESC
        ) AS global_priority_rank,

        -- Revenue tier (for filtering)
        CASE
            WHEN cs.total_revenue >= 50000 THEN 'Platinum'
            WHEN cs.total_revenue >= 20000 THEN 'Gold'
            WHEN cs.total_revenue >= 5000  THEN 'Silver'
            ELSE 'Bronze'
        END AS revenue_tier

    FROM churn_scores cs
)

SELECT
    r.*,

    -- Recommended action (business rule)
    CASE
        WHEN r.segment = 'Active'    THEN 'Upsell'
        WHEN r.segment = 'At Risk'   THEN 'Retain'
        WHEN r.segment = 'High Risk' THEN 'Retain_Urgent'
        WHEN r.segment = 'Churned'   AND r.total_revenue >= 10000 THEN 'Win_Back'
        WHEN r.segment = 'Churned'   THEN 'Ignore'
        ELSE 'Monitor'
    END AS recommended_action

FROM ranked r;

CREATE INDEX IF NOT EXISTS idx_rar_global_rank ON revenue_at_risk(global_priority_rank);
CREATE INDEX IF NOT EXISTS idx_rar_action ON revenue_at_risk(recommended_action);


-- ─────────────────────────────────────────────────────────────
-- LAYER 5: BUSINESS OUTPUTS
-- ─────────────────────────────────────────────────────────────

-- 5A. TOP 100 CHURN RISK CUSTOMERS (Action-Ready)
DROP VIEW IF EXISTS churn_risk_users;
CREATE VIEW churn_risk_users AS
SELECT
    r.global_priority_rank      AS rank,
    r.user_id,
    u.name,
    u.email,
    r.plan,
    r.country,
    r.segment,
    r.churn_score,
    r.total_revenue,
    r.revenue_at_risk,
    r.potential_recovery_value,
    r.recommended_action,
    r.recency_days,
    r.frequency,
    r.avg_order_value,
    r.revenue_tier
FROM   revenue_at_risk r
JOIN   users_clean u ON r.user_id = u.user_id
WHERE  r.segment IN ('High Risk', 'At Risk', 'Churned')
  AND  r.recommended_action != 'Ignore'
ORDER  BY r.global_priority_rank
LIMIT  100;

-- 5B. VIP CUSTOMERS (Active + High Revenue)
DROP VIEW IF EXISTS vip_customers;
CREATE VIEW vip_customers AS
SELECT
    u.user_id,
    u.name,
    u.email,
    u.plan,
    u.country,
    f.total_revenue,
    f.frequency,
    f.avg_order_value,
    f.recency_days,
    f.total_events,
    cs.churn_score,
    cs.segment,
    DENSE_RANK() OVER (ORDER BY f.total_revenue DESC) AS revenue_rank
FROM   users_clean u
JOIN   user_features f  ON u.user_id = f.user_id
JOIN   churn_scores cs  ON u.user_id = cs.user_id
WHERE  cs.segment = 'Active'
  AND  f.total_revenue >= (
          SELECT PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY total_revenue)
          FROM user_features WHERE total_revenue > 0
       )
ORDER  BY f.total_revenue DESC;

-- 5C. RE-ENGAGEMENT TARGETS (Churned but recoverable)
DROP VIEW IF EXISTS reengagement_targets;
CREATE VIEW reengagement_targets AS
SELECT
    r.user_id,
    u.name,
    u.email,
    r.plan,
    r.country,
    r.total_revenue,
    r.recency_days,
    r.churn_score,
    r.revenue_at_risk,
    r.potential_recovery_value,
    r.recommended_action
FROM   revenue_at_risk r
JOIN   users_clean u ON r.user_id = u.user_id
WHERE  r.segment = 'Churned'
  AND  r.recommended_action = 'Win_Back'
ORDER  BY r.potential_recovery_value DESC
LIMIT  500;

-- 5D. REVENUE LOSS SUMMARY (Executive KPI table)
DROP VIEW IF EXISTS revenue_loss_summary;
CREATE VIEW revenue_loss_summary AS
SELECT
    segment,
    COUNT(*)                        AS customer_count,
    ROUND(SUM(total_revenue), 2)    AS total_revenue,
    ROUND(SUM(revenue_at_risk), 2)  AS total_revenue_at_risk,
    ROUND(AVG(churn_score), 2)      AS avg_churn_score,
    ROUND(SUM(potential_recovery_value), 2) AS total_potential_recovery,
    ROUND(AVG(total_revenue), 2)    AS avg_customer_revenue
FROM   revenue_at_risk
GROUP  BY segment
ORDER  BY total_revenue_at_risk DESC;

-- 5E. COHORT REVENUE ANALYSIS
DROP VIEW IF EXISTS cohort_revenue_analysis;
CREATE VIEW cohort_revenue_analysis AS
SELECT
    f.cohort_month,
    f.plan,
    COUNT(DISTINCT f.user_id)               AS cohort_size,
    ROUND(AVG(f.total_revenue), 2)          AS avg_revenue,
    ROUND(SUM(f.total_revenue), 2)          AS total_revenue,
    ROUND(AVG(cs.churn_score), 2)           AS avg_churn_score,
    SUM(CASE WHEN cs.segment = 'Churned' THEN 1 ELSE 0 END)   AS churned_users,
    SUM(CASE WHEN cs.segment = 'Active'  THEN 1 ELSE 0 END)   AS active_users,
    ROUND(
        100.0 * SUM(CASE WHEN cs.segment = 'Churned' THEN 1 ELSE 0 END)
        / COUNT(*), 2
    )                                        AS churn_rate_pct
FROM   user_features f
JOIN   churn_scores cs ON f.user_id = cs.user_id
WHERE  f.cohort_month IS NOT NULL
GROUP  BY f.cohort_month, f.plan
ORDER  BY f.cohort_month, f.plan;
