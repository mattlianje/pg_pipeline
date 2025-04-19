CREATE TEMPORARY TABLE test_source (id INT, value TEXT);
INSERT INTO test_source VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE TEMPORARY TABLE test_destination (id INT, processed TEXT);

SELECT create_pipeline(
  'test_pipeline',
  'Pipeline for unit testing',
  '{"filter_value": "a"}',
  '{
    "extract": "SELECT * FROM test_source WHERE value = ''$(filter_value)'' OR $(filter_value) = ''all''",
    "transform": "SELECT id, value || ''_processed'' AS processed FROM ~>extract",
    "load": "INSERT INTO test_destination SELECT * FROM ~>transform RETURNING 1"
  }',
  '{"order": ["extract", "transform", "load"]}'
);

SELECT status FROM execute_pipeline('test_pipeline');

SELECT COUNT(*) FROM test_destination;
SELECT * FROM test_destination ORDER BY id;

SELECT status FROM execute_pipeline('test_pipeline', '{"filter_value": "all"}');

SELECT COUNT(*) FROM test_destination;

SELECT
  (stats->'stages'->'extract'->'records_out') AS extracted,
  (stats->'stages'->'transform'->'records_out') AS transformed,
  (stats->'stages'->'load'->'records_out') AS loaded
FROM pg_pipeline.executions
WHERE pipeline_name = 'test_pipeline'
ORDER BY started_at DESC
LIMIT 1;

DROP TABLE test_source;
DROP TABLE test_destination;
DELETE FROM pg_pipeline.pipelines WHERE name = 'test_pipeline';
DELETE FROM pg_pipeline.executions WHERE pipeline_name = 'test_pipeline';
