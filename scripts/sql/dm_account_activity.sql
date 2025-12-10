-- In file: scripts/sql/dm_account_activity.sql

-- Data Mart: dm_account_activity
-- Provides a single, wide, BI-ready table for analysis of daily transactions
-- enriched with customer and account dimensions.

CREATE OR REPLACE VIEW dm_account_activity AS
SELECT
    ft.transaction_id,
    ft.transaction_date,
    ft.transaction_amount,
    ft.transaction_type,
    ft.source_channel,

    -- Account Details
    da.account_id,
    da.account_type,

    -- Customer Details
    dc.customer_id,
    dc.region,
    dc.gender,
    dc.is_premium,
    dc.joining_date

FROM
    fact_transactions ft
JOIN
    dim_account da ON ft.account_pk = da.account_pk
JOIN
    dim_customer dc ON da.customer_id = dc.customer_id
;