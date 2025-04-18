\echo Use "CREATE EXTENSION pg_pipeline" to load this file. \quit

-- Register the hello world function
CREATE FUNCTION pipeline_hello(text)
RETURNS text
AS 'MODULE_PATHNAME', 'pipeline_hello'
LANGUAGE C STRICT;

-- Eventually, we'll add functions for creating and managing pipelines here
-- CREATE FUNCTION create_pipeline(text) RETURNS void ...
-- CREATE FUNCTION execute_pipeline(text, jsonb) RETURNS void ..
