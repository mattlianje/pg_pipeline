# pg_pipeline
**Write pipelines inside Postgres**

A simple, zero-dependency PostgreSQL extension for building data pipelines directly in your database 🐘🪄.

## Features
- Define ETL flows using plain SQL
- Simple JSON-based pipeline definition
- Customize pipeline behavior with runtime parameters
- Reference previous stage results with the ~> operator
- "Batteries-included" execution stats and pipeline health
- Pure PostgreSQL implementation

## Get started
Just run the SQL script to install the extension:
```sql
\i pg_pipeline.sql
```

## Of Note...
Most organizations don't need the complexity of external data orchestration platforms:

- External compute clusters and tools like Airflow, Dagster or Spark are overkill for 90% of data pipeline needs
- Multi-cluster/server setups create multiple points of failure
- Polling-processes outisde your DB, metric gateways and databases (although formidable and beautiful ... I'm a fan) add complexity
- If **all** your data lives in your database already, why move it out just to process it?

**pg_pipeline** caters to the 90%, where your data all lives in your DB, and you want to get started with simple, no-frills OLAP. 
When your pipelines outgrow **pg_pipeline**, you'll know.

## Core Concepts
There are just 4 things to know...
### 1. Pipeline Definition
A pipeline consists of 5 keys in a json:

- Name + Description: For identification
- Parameters: Configurable values with defaults
- Stages: Individual SQL operations to be performed
- Flow: The order of execution

### 2. Stage References
Each stage in your pipeline produces a temporary result table that subsequent stages can reference. Use the ~> operator to refer to output from previous stages:
```sql
"combined": "SELECT * FROM ~>active_users a LEFT JOIN ~>purchases p ON a.user_id = p.user_id"
```

### 3. Parameters
Use parameters with the $(param_name) syntax:
```sql
sql"active_users": "SELECT * FROM logins WHERE date > current_date - $(period)::int"
```

### 4. Execution & Monitoring
Monitor pipeline execution through the `pg_pipeline.executions` table:
```sql
SELECT 
  pipeline_name,
  started_at,
  completed_at,
  (stats->'total_duration_ms')::numeric/1000 AS duration_seconds,
  stats->'stages'->'active_users'->'records_out' AS active_users_count
FROM pg_pipeline.executions
WHERE pipeline_name = 'customer_metrics'
ORDER BY started_at DESC;
```
