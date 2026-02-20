CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(50)
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    groupname VARCHAR(50)
);

CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    customerId INT REFERENCES customers(id),
    productId INT REFERENCES products(id),
    qty INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (name, country) VALUES 
('Edvard', 'Uzbekistan'),
('Spike Spiegel', 'Japan'),
('BoJack Horseman', 'USA'),
('Diane Nguyen', 'USA'),
('Faye Valentine', 'Japan');

INSERT INTO products (name, groupname) VALUES 
('DE For Beginners', 'Books'),
('Pink Floyd - Dark Side of the Moon', 'Music'),
('Pink Floyd - The Wall', 'Music'),
('Espresso Machine Pro', 'Appliances'),
('Specialty Coffee Beans 1kg', 'Groceries'),
('Olympic Barbell 20kg', 'Sport'),
('Squat Rack Heavy Duty', 'Sport'),
('Weight Plates 25kg', 'Sport');

INSERT INTO sales (customerId, productId, qty, updated_at) VALUES 
(1, 1, 1, '2026-02-10 10:00:00'),
(2, 6, 1, '2026-02-10 11:30:00'),
(2, 8, 4, '2026-02-10 11:35:00'),
(3, 3, 2, '2026-02-11 09:15:00'),
(4, 1, 1, '2026-02-11 14:20:00'),
(1, 4, 1, '2026-02-12 10:10:00'),
(1, 5, 2, '2026-02-12 10:12:00'),
(5, 2, 1, '2026-02-13 16:45:00'),
(3, 5, 1, '2026-02-13 18:00:00'),
(2, 7, 1, '2026-02-14 12:00:00'),
(4, 6, 1, '2026-02-14 13:10:00'),
(4, 8, 2, '2026-02-14 13:15:00'),
(1, 2, 1, '2026-02-15 09:00:00'),
(5, 4, 1, '2026-02-15 11:30:00'),
(2, 5, 3, '2026-02-16 14:00:00'),
(3, 2, 1, '2026-02-16 15:20:00'),
(1, 6, 1, '2026-02-17 08:30:00'),
(1, 7, 1, '2026-02-17 08:35:00'),
(1, 8, 6, '2026-02-17 08:40:00'),
(4, 5, 2, '2026-02-17 19:00:00'),
(5, 1, 1, '2026-02-18 10:00:00'),
(3, 4, 1, '2026-02-18 12:45:00'),
(2, 2, 1, '2026-02-18 14:10:00'),
(4, 3, 1, '2026-02-18 16:30:00'),
(1, 1, -5, CURRENT_TIMESTAMP),
(1, 5, 4, CURRENT_TIMESTAMP),
(5, 8, 2, CURRENT_TIMESTAMP),
(3, 6, 1, CURRENT_TIMESTAMP),
(2, 1, 1, CURRENT_TIMESTAMP);