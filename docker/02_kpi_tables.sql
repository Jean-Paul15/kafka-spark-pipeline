-- =============================================================
--  TABLES KPI BATCH – Résultats Spark Batch ETL
--  Ajouté à init_postgres.sql (exécuté au démarrage)
-- =============================================================

-- ─── KPI E-COMMERCE BATCH ─────────────────────────────────
CREATE TABLE IF NOT EXISTS kpi_ecom_daily_category (
    id                    SERIAL PRIMARY KEY,
    event_date            DATE,
    category              VARCHAR(50),
    revenue               NUMERIC(14,2),
    transactions          INTEGER,
    unique_transactions   INTEGER,
    avg_basket            NUMERIC(10,2),
    items_sold            INTEGER,
    completed             INTEGER,
    failed                INTEGER,
    conversion_rate       NUMERIC(6,4),
    computed_at           TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kpi_ecom_date ON kpi_ecom_daily_category (event_date DESC);

CREATE TABLE IF NOT EXISTS kpi_ecom_city_device (
    id                    SERIAL PRIMARY KEY,
    city                  VARCHAR(50),
    device                VARCHAR(20),
    avg_basket            NUMERIC(10,2),
    total_revenue         NUMERIC(14,2),
    nb_transactions       INTEGER,
    avg_discount          NUMERIC(5,2),
    premium_customers     INTEGER,
    premium_rate          NUMERIC(6,4),
    computed_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kpi_ecom_hourly (
    id          SERIAL PRIMARY KEY,
    event_date  DATE,
    event_hour  INTEGER,
    nb_orders   INTEGER,
    revenue     NUMERIC(14,2),
    computed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kpi_ecom_hourly ON kpi_ecom_hourly (event_date DESC, event_hour);

CREATE TABLE IF NOT EXISTS kpi_ecom_top_products (
    id            SERIAL PRIMARY KEY,
    product_name  VARCHAR(100),
    category      VARCHAR(50),
    total_revenue NUMERIC(14,2),
    units_sold    INTEGER,
    nb_orders     INTEGER,
    avg_price     NUMERIC(10,2),
    rank          INTEGER,
    computed_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ─── KPI NYC TAXI BATCH ────────────────────────────────────
CREATE TABLE IF NOT EXISTS kpi_taxi_zones (
    id               SERIAL PRIMARY KEY,
    pickup_zone      VARCHAR(100),
    nb_trips         INTEGER,
    avg_fare         NUMERIC(8,2),
    avg_distance     NUMERIC(6,2),
    avg_tip_rate     NUMERIC(6,4),
    total_revenue    NUMERIC(14,2),
    avg_duration_min NUMERIC(6,1),
    rank             INTEGER,
    computed_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kpi_taxi_hourly (
    id          SERIAL PRIMARY KEY,
    pickup_date DATE,
    pickup_hour INTEGER,
    nb_trips    INTEGER,
    avg_fare    NUMERIC(8,2),
    revenue     NUMERIC(14,2),
    computed_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kpi_taxi_hourly ON kpi_taxi_hourly (pickup_date DESC, pickup_hour);

CREATE TABLE IF NOT EXISTS kpi_taxi_od_pairs (
    id           SERIAL PRIMARY KEY,
    pickup_zone  VARCHAR(100),
    dropoff_zone VARCHAR(100),
    nb_trips     INTEGER,
    avg_fare     NUMERIC(8,2),
    avg_distance NUMERIC(6,2),
    computed_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ─── KPI MÉTÉO BATCH ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS kpi_weather_daily (
    id               SERIAL PRIMARY KEY,
    city             VARCHAR(50),
    country          CHAR(2),
    obs_date         DATE,
    avg_temp         NUMERIC(5,1),
    min_temp         NUMERIC(5,1),
    max_temp         NUMERIC(5,1),
    avg_humidity     NUMERIC(5,1),
    avg_wind         NUMERIC(6,1),
    max_gusts        NUMERIC(6,1),
    total_precip     NUMERIC(6,1),
    max_alerts       INTEGER,
    nb_observations  INTEGER,
    computed_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_kpi_weather_daily ON kpi_weather_daily (obs_date DESC, city);

CREATE TABLE IF NOT EXISTS kpi_weather_latest (
    id                  SERIAL PRIMARY KEY,
    city                VARCHAR(50) UNIQUE,
    country             CHAR(2),
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    temperature_c       NUMERIC(5,1),
    feels_like_c        NUMERIC(5,1),
    humidity_pct        INTEGER,
    wind_speed_kmh      NUMERIC(6,1),
    wind_direction      VARCHAR(5),
    weather_description VARCHAR(100),
    precip_mm           NUMERIC(6,1),
    alert_count         INTEGER,
    fetched_at          TIMESTAMPTZ,
    computed_at         TIMESTAMPTZ DEFAULT NOW()
);
