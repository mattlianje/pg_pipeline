-- Demo data
CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  user_id INT,
  product TEXT,
  amount NUMERIC,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
  user_id INT PRIMARY KEY,
  name TEXT,
  plan TEXT,
  signup_date DATE
);

INSERT INTO users VALUES
  (1, 'Alice',   'pro',    '2025-01-10'),
  (2, 'Bob',     'free',   '2025-06-01'),
  (3, 'Charlie', 'pro',    '2025-03-15'),
  (4, 'Diana',   'team',   '2024-11-20'),
  (5, 'Eve',     'free',   '2025-09-01'),
  (6, 'Frank',   'team',   '2025-02-14'),
  (7, 'Grace',   'pro',    '2025-07-22'),
  (8, 'Hank',    'free',   '2025-12-01');

INSERT INTO orders (user_id, product, amount, created_at)
SELECT
  (random() * 7 + 1)::int,
  (ARRAY['widget','gadget','doohickey','gizmo','thingamajig'])[floor(random()*5+1)::int],
  round((random() * 200 + 5)::numeric, 2),
  NOW() - (random() * 30 || ' days')::interval
FROM generate_series(1, 500);

-- Pipeline 1: Revenue by product
SELECT create_pipeline('revenue_by_product', 'Revenue breakdown by product', '{"days": "7"}', '{"recent_orders": "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL ''$(days) days''", "by_product": "SELECT product, COUNT(*) AS num_orders, SUM(amount) AS revenue, ROUND(AVG(amount), 2) AS avg_order FROM #recent_orders GROUP BY product ORDER BY revenue DESC"}', '{"order": ["recent_orders", "by_product"]}');

-- Pipeline 2: Spend by plan tier
SELECT create_pipeline('spend_by_plan', 'Spend segmented by plan tier', '{"days": "14"}', '{"window": "SELECT o.user_id, u.name, u.plan, o.product, o.amount FROM orders o JOIN users u USING (user_id) WHERE o.created_at > NOW() - INTERVAL ''$(days) days''", "per_user": "SELECT user_id, name, plan, COUNT(*) AS orders, SUM(amount) AS total_spend FROM #window GROUP BY 1, 2, 3", "per_plan": "SELECT plan, COUNT(*) AS users, SUM(orders) AS total_orders, SUM(total_spend) AS total_revenue, ROUND(AVG(total_spend), 2) AS avg_spend_per_user FROM #per_user GROUP BY 1 ORDER BY total_revenue DESC"}', '{"order": ["window", "per_user", "per_plan"]}');

-- Run them a few times with different params
SELECT execute_pipeline('revenue_by_product', '{"days": "3"}');
SELECT execute_pipeline('revenue_by_product', '{"days": "7"}');
SELECT execute_pipeline('revenue_by_product', '{"days": "14"}');
SELECT execute_pipeline('revenue_by_product', '{"days": "30"}');

SELECT execute_pipeline('spend_by_plan', '{"days": "7"}');
SELECT execute_pipeline('spend_by_plan', '{"days": "14"}');
SELECT execute_pipeline('spend_by_plan', '{"days": "30"}');
