CREATE DATABASE IF NOT EXISTS nyc_taxi_dw;

CREATE TABLE IF NOT EXISTS nyc_taxi_dw.dim_time
(
    timestamp_id     DateTime,
    date             Date,
    year             Int32,
    month            Int32,
    day              Int32,
    hour             Int32,
    minute           Int32,
    day_of_week      String,
    is_weekend       UInt8
)
ENGINE = MergeTree
ORDER BY (timestamp_id);

CREATE TABLE IF NOT EXISTS nyc_taxi_dw.dim_vendor
(
    vendor_id     Int32,
    provider_name String
)
ENGINE = MergeTree
ORDER BY (vendor_id);

CREATE TABLE IF NOT EXISTS nyc_taxi_dw.dim_payment
(
    payment_id        Int32,
    payment_type_name String,
    description       String
)
ENGINE = MergeTree
ORDER BY (payment_id);

CREATE TABLE IF NOT EXISTS nyc_taxi_dw.dim_ratecode
(
    rate_code_id   Int32,
    rate_code_name String,
    description    String
)
ENGINE = MergeTree
ORDER BY (rate_code_id);

CREATE TABLE IF NOT EXISTS nyc_taxi_dw.dim_location
(
    location_id  Int32,
    borough      String,
    zone         String,
    service_zone String
)
ENGINE = MergeTree
ORDER BY (location_id);

CREATE TABLE IF NOT EXISTS nyc_taxi_dw.fact_trip
(
    fact_trip_sk          UInt64,
    pickup_time_sk        DateTime,
    dropoff_time_sk       DateTime,
    vendor_sk             Int32,
    ratecode_sk           Int32,
    payment_type_sk       Int32,
    pickup_location_sk    Int32,
    dropoff_location_sk   Int32,

    passenger_count       Int32,
    trip_distance_miles   Float64,
    store_and_fwd_flag    String,
    fare_amount           Float64,
    extra                 Float64,
    mta_tax               Float64,
    tip_amount            Float64,
    tolls_amount          Float64,
    improvement_surcharge Float64,
    total_amount          Float64,
    congestion_surcharge  Float64,
    airport_fee           Float64,
    cbd_congestion_fee    Float64,

    trip_time_seconds     Int64,
    created_at_sk         DateTime
)
ENGINE = MergeTree
ORDER BY (pickup_time_sk, dropoff_time_sk, vendor_sk);