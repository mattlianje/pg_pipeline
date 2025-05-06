<p align="center">
  <img src="pix/pg_pipeline_with_github.png" width="350" alt="pg_pipeline logo">
</p>

# pg_pipeline
**Powerful, database-native pipelines** 

A PostgreSQL extension to build, run, monitor pipelines directly in your database 🐘🪄

## Features
- Simple JSON-based pipeline definition
- Zero dependencies, no external tools
- Config-driven pipelines
- Reference previous stage results with `~>`
- "Batteries-included" execution stats

## Get started
Just run the SQL script to install the extension:
```sql
\i pg_pipeline.sql
```

## Of Note...
If **all** your data lives in your database already, why move it out just to process it?

Many teams and use-cases don't need the overhead of external data orchestration platforms / cluster compute.
**pg_pipeline** caters to the 90%, where all your data lives in your DB, and you want to get started with simple, no-frills OLAP.

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

