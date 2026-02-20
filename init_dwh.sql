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


-- Loyalty points calculation function
CREATE OR REPLACE FUNCTION dwh.calculate_loyalty_points(qty INT)
RETURNS INT AS $$
BEGIN
    IF qty >= 3 THEN 
        RETURN qty * 50; -- Wholesale purchase: 50 points per piece
    ELSE 
        RETURN qty * 10; -- Regular purchase: 10 points per piece
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Sales Processing Procedure with Cursor
CREATE OR REPLACE PROCEDURE dwh.process_sales_loyalty()
LANGUAGE plpgsql
AS $$
DECLARE
    sales_cursor CURSOR FOR SELECT id, qty FROM dwh.dwh_fact_sales WHERE loyalty_points = 0 OR loyalty_points IS NULL;
    v_id INT;
    v_qty INT;
    v_points INT;
    v_error_msg TEXT;
BEGIN
    OPEN sales_cursor;
    
    LOOP
        FETCH sales_cursor INTO v_id, v_qty;
        EXIT WHEN NOT FOUND;
        
        BEGIN -- TRY
            IF v_qty < 0 THEN
                RAISE EXCEPTION 'Critical Error: Negative Quantity on Sale ID %', v_id;
            END IF;

            v_points := dwh.calculate_loyalty_points(v_qty);
            
            UPDATE dwh.dwh_fact_sales 
            SET loyalty_points = v_points 
            WHERE id = v_id;
                        
        EXCEPTION -- CATCH
            WHEN OTHERS THEN
                v_error_msg := SQLERRM;
                INSERT INTO dwh.etl_logs (process_name, status, error_message)
                VALUES ('procedure_loyalty_calc', 'FAILED', v_error_msg);
        END;
    END LOOP;
    
    CLOSE sales_cursor;
    
    INSERT INTO dwh.etl_logs (process_name, status, error_message)
    VALUES ('procedure_loyalty_calc', 'SUCCESS', 'All points calculated and saved successfully');
END;
$$;