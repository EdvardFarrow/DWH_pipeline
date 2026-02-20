CREATE SCHEMA mrr;
CREATE SCHEMA stg;
CREATE SCHEMA dwh;

-- MRR
CREATE TABLE mrr.mrr_dim_customers (
    id INT, name VARCHAR(100), country VARCHAR(50)
);

CREATE TABLE mrr.mrr_dim_products (
    id INT, name VARCHAR(100), groupname VARCHAR(50)
);

CREATE TABLE mrr.mrr_fact_sales (
    id INT, customerId INT, productId INT, qty INT, updated_at TIMESTAMP
);

-- STG
CREATE TABLE stg.stg_dim_customers (
    id INT, name VARCHAR(100), country VARCHAR(50)
);

CREATE TABLE stg.stg_dim_products (
    id INT, name VARCHAR(100), groupname VARCHAR(50)
);

CREATE TABLE stg.stg_fact_sales (
    id INT, customerId INT, productId INT, qty INT, updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.rejected_sales (
    id INT, 
    customerId INT, 
    productId INT, 
    qty INT, 
    updated_at TIMESTAMP,
    reject_reason VARCHAR(255),
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DWH
CREATE TABLE dwh.dwh_dim_customers (
    id INT PRIMARY KEY, name VARCHAR(100), country VARCHAR(50)
);

CREATE TABLE dwh.dwh_dim_products (
    id INT PRIMARY KEY, name VARCHAR(100), groupname VARCHAR(50)
);

CREATE TABLE dwh.dwh_fact_sales (
    id INT PRIMARY KEY, 
    customerId INT REFERENCES dwh.dwh_dim_customers(id), 
    productId INT REFERENCES dwh.dwh_dim_products(id), 
    qty INT, 
    updated_at TIMESTAMP,
    loyalty_points INT DEFAULT 0
);

-- HWM & logs
CREATE TABLE dwh.high_water_mark (
    table_name VARCHAR(50) PRIMARY KEY,
    last_update_date TIMESTAMP
);

-- Init HWM 
INSERT INTO dwh.high_water_mark (table_name, last_update_date) 
VALUES ('sales', '1900-01-01 00:00:00');

-- Airflow logs
CREATE TABLE dwh.etl_logs (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100),
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    error_message TEXT
);