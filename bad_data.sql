-- TRIM and UPPER check
INSERT INTO customers (id, name, country) 
VALUES (101, '   Dirty Harry   ', 'usa');

INSERT INTO sales (id, customerid, productid, qty, updated_at) 
VALUES (601, 101, 1, 5, CURRENT_TIMESTAMP);

-- Zero Value
INSERT INTO sales (id, customerid, productid, qty, updated_at) 
VALUES (602, 101, 1, 0, CURRENT_TIMESTAMP);

-- Negative values
INSERT INTO sales (id, customerid, productid, qty, updated_at) 
VALUES (603, 101, 2, -50, CURRENT_TIMESTAMP);