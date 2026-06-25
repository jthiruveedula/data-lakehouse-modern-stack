-- Source database schema for local CDC development
-- Logical replication is enabled via postgres command flags in docker-compose.yml

CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(255) UNIQUE NOT NULL,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    tier        VARCHAR(20) DEFAULT 'bronze' CHECK (tier IN ('gold', 'silver', 'bronze')),
    region      VARCHAR(50),
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(id),
    amount      NUMERIC(12, 2) NOT NULL,
    status      VARCHAR(20) DEFAULT 'pending'
                  CHECK (status IN ('pending','confirmed','shipped','delivered','returned','cancelled')),
    order_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

-- Logical replication publication for Debezium CDC
CREATE PUBLICATION lakehouse_pub FOR TABLE customers, orders;

-- Seed data
INSERT INTO customers (email, first_name, last_name, tier, region) VALUES
    ('alice@example.com',   'Alice',   'Smith',   'gold',   'us-east'),
    ('bob@example.com',     'Bob',     'Jones',   'silver', 'us-west'),
    ('carol@example.com',   'Carol',   'Brown',   'bronze', 'eu-west'),
    ('dave@example.com',    'Dave',    'Wilson',  'gold',   'ap-south'),
    ('eve@example.com',     'Eve',     'Davis',   'silver', 'us-east')
ON CONFLICT DO NOTHING;

INSERT INTO orders (customer_id, amount, status, order_date) VALUES
    (1, 250.00, 'delivered', CURRENT_DATE - 5),
    (1, 89.99,  'shipped',   CURRENT_DATE - 2),
    (2, 430.00, 'confirmed', CURRENT_DATE - 1),
    (3, 15.00,  'pending',   CURRENT_DATE),
    (4, 1200.00,'delivered', CURRENT_DATE - 10),
    (5, 75.50,  'returned',  CURRENT_DATE - 7)
ON CONFLICT DO NOTHING;
