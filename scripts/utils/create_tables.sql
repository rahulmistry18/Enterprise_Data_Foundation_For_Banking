-- In file: enterprise_data_foundation_for_banking/scripts/sql/create_tables.sql

-- Drop tables if they exist to allow clean restarts
DROP TABLE IF EXISTS banking_dw.dim_customer CASCADE;
DROP TABLE IF EXISTS banking_dw.dim_account CASCADE;
DROP TABLE IF EXISTS banking_dw.fact_transactions CASCADE;
DROP TABLE IF EXISTS banking_dw.stg_customer_data;
DROP TABLE IF EXISTS banking_dw.stg_fact_data;
DROP VIEW IF EXISTS banking_dw.dm_account_activity;

-- Create Schema for the Data Warehouse
CREATE SCHEMA IF NOT EXISTS banking_dw;

-- =================================================================================
-- STAGING TABLES (Temporary landing areas for raw data)
-- =================================================================================

-- Staging Table for Customer Master Data
CREATE TABLE banking_dw.stg_customer_data (
    customer_id VARCHAR(50),
    gender VARCHAR(10),
    region VARCHAR(50),
    joining_date DATE,
    is_premium BOOLEAN
);

-- Staging Table for Transaction and Account Data
CREATE TABLE banking_dw.stg_fact_data (
    transaction_id VARCHAR(100),
    account_id VARCHAR(100),
    customer_id VARCHAR(50),  -- Added customer_id for easier lookups
    transaction_date DATE,
    amount NUMERIC(10, 2),
    type VARCHAR(20),
    source_channel VARCHAR(20)
);

-- =================================================================================
-- DIMENSION TABLES (Conformed Dimensions)
-- =================================================================================

-- 1. Dimension Customer
CREATE TABLE banking_dw.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    gender VARCHAR(10),
    region VARCHAR(50),
    joining_date DATE,
    is_premium BOOLEAN,
    valid_from TIMESTAMP DEFAULT NOW(),
    valid_to TIMESTAMP
);

-- 2. Dimension Account (No data is loaded into this table; it's joined from Spark transformation)
-- In a real scenario, this would be loaded as a SCD Type 2 dimension.
CREATE TABLE banking_dw.dim_account (
    account_sk SERIAL PRIMARY KEY,
    account_id VARCHAR(100) UNIQUE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    account_type VARCHAR(20),
    current_balance NUMERIC(15, 2),
    valid_from TIMESTAMP DEFAULT NOW(),
    valid_to TIMESTAMP
);

-- =================================================================================
-- FACT TABLE (Transactional Data)
-- =================================================================================

CREATE TABLE banking_dw.fact_transactions (
    transaction_sk SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    -- Foreign Keys
    customer_sk INTEGER NOT NULL REFERENCES banking_dw.dim_customer (customer_sk),
    account_sk INTEGER NOT NULL REFERENCES banking_dw.dim_account (account_sk),
    -- Transaction Details
    transaction_date DATE NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    type VARCHAR(20) NOT NULL,
    source_channel VARCHAR(20)
);