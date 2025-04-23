<p align="center">
  <img src="pix/pg_pipeline_with_github.png" width="350" alt="pg_pipeline logo">
</p>

# pg_pipeline
**Simple, flow-style pipelines in Postgres**

A simple PostgreSQL extension for building pipelines directly in your database 🐘🪄

## Features
- Simple JSON-based pipeline definition
- Config-driven pipelines
- Reference previous stage results (flow-style) with the `~>` operator
- "Batteries-included" execution stats

## Get started
Just run the SQL script to install the extension:
```sql
\i pg_pipeline.sql
```

## Of Note...
Many teams don't need the overhead of external data orchestration platforms / cluster compute:

- External compute clusters and tools like Airflow, Dagster or Spark are overkill for 90% of data pipeline needs
- Multi-cluster/server/technology setups create multiple points of failure
- Polling-processes outisde your DB and metric databases (although formidable and beautiful) add complexity
- If **all** your data lives in your database already, why move it out just to process it?

**pg_pipeline** caters to the 90%, where all your data lives in your DB, and you want to get started with simple, no-frills OLAP. 
When your pipelines outgrows **pg_pipeline**, you'll know it.

## Core Concepts
There are just 4 things to know...
### Pipeline
A pipeline consists of 5 keys in a json:

- Name + Description: For identification
- Parameters: Configurable values with defaults
- Stages: Individual SQL operations to be performed
- Flow: The order of execution

### Stage
Each stage in your pipeline produces a temporary result table that subsequent stages can reference. Use the `~>` operator to refer to output from previous stages:
```sql
SELECT * FROM ~>active_users a LEFT JOIN ~>purchases p ON a.user_id = p.user_id
```

### Parameters
Make your pipelines config-driven with `$(param_name)` syntax:
```sql
SELECT * FROM logins WHERE date > current_date - $(period)::int
```

### Execution

Everytime you execute a pipeline with `pipeline_execute`, run info with records processed and time-elapsed per stage
are written to the `pg_pipeline.executions` table.
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

