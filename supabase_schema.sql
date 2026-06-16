-- ============================================================
-- Esquema Supabase — ETL NYC TLC  (ejecutar en SQL Editor)
-- ============================================================

-- 1. gold_trips_clean  (muestra representativa de silver)
CREATE TABLE IF NOT EXISTS gold_trips_clean (
    trip_id                 TEXT        PRIMARY KEY,
    service_type            TEXT,
    vendor_id               TEXT,
    pickup_datetime         TIMESTAMPTZ,
    dropoff_datetime        TIMESTAMPTZ,
    passenger_count         FLOAT8,
    trip_distance           FLOAT8,
    pickup_location_id      BIGINT,
    dropoff_location_id     BIGINT,
    payment_type            TEXT,
    fare_amount             FLOAT8,
    extra_amount            FLOAT8,
    mta_tax                 FLOAT8,
    tip_amount              FLOAT8,
    tolls_amount            FLOAT8,
    total_amount            FLOAT8,
    congestion_surcharge    FLOAT8,
    airport_fee             FLOAT8,
    improvement_surcharge   FLOAT8,
    source_file             TEXT,
    ingestion_timestamp     TIMESTAMPTZ,
    quality_status          TEXT,
    trip_duration_minutes   FLOAT8,
    average_speed_mph       FLOAT8,
    fare_per_mile           FLOAT8,
    tip_percentage          FLOAT8,
    is_airport_trip         BOOLEAN,
    is_suspicious_trip      BOOLEAN,
    processing_date         DATE,
    year                    INTEGER,
    month                   INTEGER
);

-- 2. gold_daily_revenue
CREATE TABLE IF NOT EXISTS gold_daily_revenue (
    service_type            TEXT,
    trip_date               DATE,
    total_trips             BIGINT,
    total_revenue           FLOAT8,
    average_fare            FLOAT8,
    average_tip             FLOAT8,
    average_trip_distance   FLOAT8,
    average_trip_duration   FLOAT8,
    rejected_records        BIGINT      DEFAULT 0,
    quality_percentage      FLOAT8      DEFAULT 100.0,
    PRIMARY KEY (service_type, trip_date)
);

-- 3. gold_location_performance
CREATE TABLE IF NOT EXISTS gold_location_performance (
    service_type            TEXT,
    pickup_location_id      FLOAT8,
    dropoff_location_id     FLOAT8,
    total_trips             BIGINT,
    total_revenue           FLOAT8,
    average_fare            FLOAT8,
    average_distance        FLOAT8,
    average_duration        FLOAT8,
    suspicious_trip_count   BIGINT      DEFAULT 0,
    PRIMARY KEY (service_type, pickup_location_id, dropoff_location_id)
);

-- 4. quality_metrics  (ya debe existir — verificar)
CREATE TABLE IF NOT EXISTS quality_metrics (
    process_id              TEXT,
    service_type            TEXT,
    year                    INTEGER,
    month                   INTEGER,
    total_records           BIGINT,
    valid_records           BIGINT,
    rejected_records        BIGINT,
    duplicate_records       BIGINT,
    null_critical_records   BIGINT,
    suspicious_records      BIGINT,
    quality_percentage      FLOAT8,
    processed_at            TIMESTAMPTZ,
    PRIMARY KEY (process_id, service_type, year, month)
);

-- 5. quality_rejected_records  (ya debe existir — verificar)
CREATE TABLE IF NOT EXISTS quality_rejected_records (
    process_id              TEXT,
    trip_id                 TEXT,
    rejection_rule          TEXT,
    service_type            TEXT,
    year                    INTEGER,
    month                   INTEGER,
    PRIMARY KEY (process_id, trip_id, rejection_rule)
);

-- 6. audit_file_inventory  (ya debe existir — verificar)
CREATE TABLE IF NOT EXISTS audit_file_inventory (
    process_id              TEXT,
    file_name               TEXT,
    service_type            TEXT,
    year                    INTEGER,
    month                   INTEGER,
    read_status             TEXT,
    num_rows                BIGINT,
    file_size_mb            FLOAT8,
    ingestion_timestamp     TIMESTAMPTZ,
    error_message           TEXT,
    PRIMARY KEY (process_id, file_name)
);

-- 7. load_audit  (ya debe existir — verificar)
CREATE TABLE IF NOT EXISTS load_audit (
    id                      BIGSERIAL   PRIMARY KEY,
    process_id              TEXT,
    table_name              TEXT,
    rows_inserted           BIGINT,
    status                  TEXT,
    load_timestamp          TIMESTAMPTZ DEFAULT NOW(),
    error_message           TEXT
);
