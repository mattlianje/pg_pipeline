/***
Example number 2:
CREATE TABLE recent_sales (
  id SERIAL PRIMARY KEY,
  product_id INT,
  quantity INT,
  sale_date DATE
);

CREATE TABLE product_performance (
  report_date DATE,
  product_id INT,
  total_sold INT,
  PRIMARY KEY (report_date, product_id)
);

INSERT INTO recent_sales (product_id, quantity, sale_date) VALUES
  (101, 5, CURRENT_DATE - 1),
  (102, 3, CURRENT_DATE - 2),
  (101, 2, CURRENT_DATE - 3),
  (103, 8, CURRENT_DATE - 4),
  (102, 1, CURRENT_DATE - 5),
  (101, 4, CURRENT_DATE - 6);

-- Step 1: Create a pipeline with `create_pipeline`
SELECT create_pipeline(
  'sales_summary',                 -- Set pipeline name
  'Daily product sales summary',   -- Add a description
  '{"days_ago": "7"}',             -- Create params w/ default values

  -- Set your stages. Use params with $(param_name), previous stages with ~>stage_name
  '{
    "get_sales": "SELECT * FROM recent_sales WHERE sale_date > CURRENT_DATE - $(days_ago)",
    "summarize": "SELECT product_id, SUM(quantity) AS total_sold FROM ~>get_sales GROUP BY product_id",
    "save": "INSERT INTO product_performance SELECT CURRENT_DATE, product_id, total_sold FROM ~>summarize"
  }',
  '{"order": ["get_sales", "summarize", "save"]}' -- Sequence your pipeline stages
);

-- Step 2: Run your pipeline w/ `execute_pipeline`
SELECT execute_pipeline('sales_summary', '{"days_ago": "6"}');

-- Query past runs, row counts, etc in pipeline.stage_exeuctions
SELECT pipeline_name, execution_id, started_at, stage_name, duration_ms, records_out
FROM pipeline.stage_executions ORDER BY execution_id DESC LIMIT 3;

*/
