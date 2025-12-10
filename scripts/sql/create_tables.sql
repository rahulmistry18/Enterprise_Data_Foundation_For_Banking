-- In file: scripts/sql/create_tables.sql

-- ====================================================================
-- STAGING LAYER (RAW DATA LANDING ZONE)
-- These tables directly mirror the structure of the input CSVs for ingestion.
-- ====================================================================

CREATE TABLE IF NOT EXISTS stg_customer_master (
    customer_id VARCHAR(100),
    gender VARCHAR(10),
    region VARCHAR(50),
    joining_date DATE,
    is_premium BOOLEAN
);

-- Staging table for transactional data, cleared after loading fact table
CREATE TABLE IF NOT EXISTS stg_transaction_data (
    transaction_id VARCHAR(100),
    account_id VARCHAR(100),
    transaction_date DATE,
    amount DECIMAL(18, 2),
    type VARCHAR(50),
    source_channel VARCHAR(50)
);

-- ====================================================================
-- CORE DWH LAYER (STAR SCHEMA)
-- Optimized for querying and analytics.
-- ====================================================================

-- 1. DIMENSION TABLE: DIM_CUSTOMER
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_pk SERIAL PRIMARY KEY,    -- Surrogate Key
    customer_id VARCHAR(100) UNIQUE,   -- Natural Key
    gender VARCHAR(10),
    region VARCHAR(50),
    joining_date DATE,
    is_premium BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);

-- 2. DIMENSION TABLE: DIM_ACCOUNT
CREATE TABLE IF NOT EXISTS dim_account (
    account_pk SERIAL PRIMARY KEY,
    account_id VARCHAR(100) UNIQUE,
    customer_id VARCHAR(100) REFERENCES dim_customer(customer_id), -- FK to Customer Dimension
    account_type VARCHAR(50)
);

-- 3. FACT TABLE: FACT_TRANSACTIONS
-- Partitioned by transaction_date for SQL Optimization
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    account_pk INTEGER,
    transaction_date DATE,
    transaction_amount DECIMAL(18, 2),
    transaction_type VARCHAR(50),
    source_channel VARCHAR(50),
    audit_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (transaction_date);

-- Initial Partitioning (Airflow will manage creation in production)
CREATE TABLE IF NOT EXISTS fact_transactions_202501 PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE IF NOT EXISTS fact_transactions_202502 PARTITION OF fact_transactions
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');