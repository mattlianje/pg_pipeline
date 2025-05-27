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
Just run the SQL script to install the extension:
```sql
\i pg_pipeline.sql
```

## Of Note...
**pg_pipeline** is just a few PL/pgSQL functions that let you build config-driven query pipelines with JSON. 

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

