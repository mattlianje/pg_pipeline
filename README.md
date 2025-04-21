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
- Resource-intensive setups require specialized DevOps knowledge, infrastructure management, and introduce multiple points of failure
- Monitoring dashboards in Grafana, monitoring polling processes, and metrics databases (although formidable and beautiful ... I'm a fan) add even more complexity
- If all your data lives in your database already, why move it out just to process it?

**pg_pipeline** caters to the 90%, where:

- You already have PostgreSQL as your data store
- Your transformations fit comfortably within SQL
- You want simple observability without complex monitoring stacks
- You need parameterizable, reusable data flows
- You prefer maintaining one technology stack instead of several

When your pipelines outgrow **pg_pipeline**, you'll know.
