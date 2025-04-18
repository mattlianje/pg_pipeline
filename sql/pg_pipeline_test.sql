-- Test that extension can be created
CREATE EXTENSION pg_pipeline;

-- Test the hello world function
SELECT pipeline_hello('PostgreSQL');
SELECT pipeline_hello(NULL);

-- Clean up
DROP EXTENSION pgpipeline;
