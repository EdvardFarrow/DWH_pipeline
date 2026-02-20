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