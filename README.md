<p align="center">
  <img src="pix/pg_pipeline_demo.png" width="700">
</p>

# <img src="pix/pg_pipeline.png" width="50"> pg_pipeline
**Write pipelines inside Postgres** 

A lightweight PostgresQL library to build, store and run pipelines directly in your database 🐘🪄.
Part of [d4](https://github.com/mattlianje/d4)

## Features
- Simple JSON-based pipeline definition
- Zero dependencies, no external tools
- Config-driven pipelines
- Reference previous stage results with `~>`
- Execution stats, row counts for free

## Get started
Just run the SQL script to install the lib:
```sql
\i pg_pipeline.sql
```

## Of Note...
At the end of the day, **pg_pipeline** is just a few PL/pgSQL functions that let you build config-driven query pipelines with JSON. 

It targets the 90% use case where your data lives in your database and you want simple, no-frills data processing without the complexity of external workflow schedulers or cluster-compute engines.

## Core Concepts
There are just 4 things to know...
### Pipeline definition
A pipeline is created using create_pipeline() with 5 parameters:

- `name`: Pipeline identifier (string)
- `description`: Pipeline description (string)
- `parameters`: Configurable values with defaults (JSON string)
- `stages`: Individual SQL operations (JSON string)
- `execution_order`: Execution order specification (JSON string with "order" array)

### Stage references
Each stage produces a temporary result table. Reference previous stages using the `~>` operator:
```sql
SELECT * FROM ~>active_users a LEFT JOIN ~>purchases p ON a.user_id = p.user_id
```

### Parameters
Make your pipelines config-driven with `$(param_name)` syntax:
```sql
SELECT * FROM logins WHERE date > current_date - $(period)
```

### Execution tracking
Every `execute_pipeline()` call logs execution metadata to `pipeline.stage_executions`, including records processed and duration per stage.

## FAQ

**Q: Do I need to install anything outside Postgres?**  
Nope. It’s 100% pure SQL/PLpgSQL. Just run the install script.

**Q: What does `~>` actually do?**  
It expands to a temp table created by a previous stage — like `pipeline_tmp_<stage>`.

**Q: How are parameters handled?**  
They're string-substituted into your SQL before execution. Use `$(param_name)` and pass values as JSON.

**Q: Is this safe for production?**  
Yep - but the project fledgling.

**Q: Can I use `pg_cron` or triggers to schedule pipelines?**  
Absolutely. Use `pg_cron` for scheduling, or call `execute_pipeline()` from app logic.

**Q: What happens if a stage fails?**  
Execution halts immediately. Logs still write to `pipeline.stage_executions`, and prior temp tables are preserved.

## Full example
```sql
-- Setup: Tables for raw data and output
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

-- Sample data
INSERT INTO recent_sales (product_id, quantity, sale_date) VALUES
  (101, 5, CURRENT_DATE - 1),
  (102, 3, CURRENT_DATE - 2),
  (101, 2, CURRENT_DATE - 3),
  (103, 8, CURRENT_DATE - 4),
  (102, 1, CURRENT_DATE - 5),
  (101, 4, CURRENT_DATE - 6);

-- Create the pipeline
SELECT create_pipeline(
  'sales_summary',
  'Daily product sales summary',
  '{"days_ago": "7"}',
  '{
    "get_sales": "SELECT * FROM recent_sales WHERE sale_date > CURRENT_DATE - $(days_ago)",
    "summarize": "SELECT product_id, SUM(quantity) AS total_sold FROM ~>get_sales GROUP BY product_id",
    "save": "INSERT INTO product_performance SELECT CURRENT_DATE, product_id, total_sold FROM ~>summarize"
  }',
  '{"order": ["get_sales", "summarize", "save"]}'
);

-- Run the pipeline with a param override
SELECT execute_pipeline('sales_summary', '{"days_ago": "6"}');

-- Inspect recent executions
SELECT pipeline_name, execution_id, stage_name, duration_ms, records_out
FROM pipeline.stage_executions
ORDER BY execution_id DESC
LIMIT 3;
```

