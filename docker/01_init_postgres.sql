-- =============================================================
--  INIT POSTGRESQL – Tables pour Grafana + Spark Streaming
-- =============================================================

-- ─── E-COMMERCE ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ecommerce_events (
    id               SERIAL PRIMARY KEY,
    transaction_id   VARCHAR(36),
    event_time       TIMESTAMPTZ,
    city             VARCHAR(50),
    age_group        VARCHAR(10),
    is_premium       BOOLEAN,
    category         VARCHAR(50),
    product_name     VARCHAR(100),
    unit_price       NUMERIC(10,2),
    quantity         INTEGER,
    discount_pct     INTEGER,
    total_amount     NUMERIC(10,2),
    payment_method   VARCHAR(30),
    status           VARCHAR(20),
    device           VARCHAR(20),
    acquisition_source VARCHAR(30),
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ecom_time     ON ecommerce_events (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_ecom_category ON ecommerce_events (category);
CREATE INDEX IF NOT EXISTS idx_ecom_status   ON ecommerce_events (status);

-- ─── NYC TAXI ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS taxi_events (
    id               SERIAL PRIMARY KEY,
    trip_id          VARCHAR(36),
    pickup_time      TIMESTAMPTZ,
    dropoff_time     TIMESTAMPTZ,
    pickup_zone      VARCHAR(100),
    dropoff_zone     VARCHAR(100),
    pickup_lat       DOUBLE PRECISION,
    pickup_lon       DOUBLE PRECISION,
    dropoff_lat      DOUBLE PRECISION,
    dropoff_lon      DOUBLE PRECISION,
    passenger_count  INTEGER,
    distance_miles   NUMERIC(8,2),
    duration_minutes INTEGER,
    base_fare        NUMERIC(10,2),
    tip_amount       NUMERIC(10,2),
    total_amount     NUMERIC(10,2),
    payment_type     VARCHAR(20),
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_taxi_time     ON taxi_events (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_taxi_zone_pu  ON taxi_events (pickup_zone);
CREATE INDEX IF NOT EXISTS idx_taxi_zone_do  ON taxi_events (dropoff_zone);

-- ─── MÉTÉO ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS weather_events (
    id               SERIAL PRIMARY KEY,
    event_id         VARCHAR(36),
    fetched_at       TIMESTAMPTZ,
    city             VARCHAR(50),
    country          CHAR(2),
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    temperature_c    NUMERIC(5,1),
    feels_like_c     NUMERIC(5,1),
    humidity_pct     INTEGER,
    pressure_hpa     NUMERIC(7,1),
    cloud_cover_pct  INTEGER,
    visibility_m     NUMERIC(10,1),
    uv_index         NUMERIC(4,1),
    weather_description VARCHAR(100),
    precip_mm        NUMERIC(6,1),
    wind_speed_kmh   NUMERIC(6,1),
    wind_direction   VARCHAR(5),
    wind_gusts_kmh   NUMERIC(6,1),
    alert_count      INTEGER DEFAULT 0,
    alerts_json      TEXT,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_weather_time  ON weather_events (ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_city  ON weather_events (city, fetched_at DESC);

-- ─── VUES GRAFANA – KPIs agrégés ──────────────────────────

-- CA en temps réel (dernières 24h par heure)
CREATE OR REPLACE VIEW v_ecom_revenue_per_hour AS
SELECT
    date_trunc('hour', ingested_at) AS hour,
    SUM(total_amount)               AS revenue,
    COUNT(*)                        AS transactions,
    AVG(total_amount)               AS avg_basket,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) AS conversion_rate
FROM ecommerce_events
WHERE ingested_at >= NOW() - INTERVAL '24 hours'
GROUP BY 1 ORDER BY 1;

-- CA par catégorie (dernières 2h)
CREATE OR REPLACE VIEW v_ecom_by_category AS
SELECT
    category,
    COUNT(*)          AS nb_transactions,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_amount,
    SUM(quantity)     AS total_items_sold
FROM ecommerce_events
WHERE ingested_at >= NOW() - INTERVAL '2 hours'
  AND status = 'completed'
GROUP BY category ORDER BY total_revenue DESC;

-- Top zones taxi (dernière heure)
CREATE OR REPLACE VIEW v_taxi_top_zones AS
SELECT
    pickup_zone,
    COUNT(*)                 AS nb_trips,
    AVG(total_amount)        AS avg_fare,
    AVG(distance_miles)      AS avg_distance,
    SUM(total_amount)        AS total_revenue
FROM taxi_events
WHERE ingested_at >= NOW() - INTERVAL '1 hour'
GROUP BY pickup_zone ORDER BY nb_trips DESC LIMIT 15;

-- Dernières mesures météo par ville
CREATE OR REPLACE VIEW v_weather_latest AS
SELECT DISTINCT ON (city)
    city, country, latitude, longitude,
    temperature_c, feels_like_c, humidity_pct,
    wind_speed_kmh, wind_direction, wind_gusts_kmh,
    weather_description, precip_mm, alert_count, fetched_at
FROM weather_events
ORDER BY city, fetched_at DESC;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO bigdata;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO bigdata;
