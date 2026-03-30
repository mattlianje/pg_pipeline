\set ON_ERROR_STOP on
\set QUIET on
\pset tuples_only on
\pset format unaligned

-- Run: psql -f test.sql

\echo '--- Installing pg_pipeline ---'
\i pg_pipeline.sql

-- ============================================================
-- Setup: test tables
-- ============================================================
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

-- ============================================================
-- Test 1: Create pipeline
-- ============================================================
\echo '--- Test 1: create_pipeline ---'
DO $$
DECLARE
  pid INT;
BEGIN
  pid := create_pipeline(
    'sales_summary',
    'Daily product sales summary',
    '{"days_ago": "7"}',
    '{
      "get_sales": "SELECT * FROM recent_sales WHERE sale_date > CURRENT_DATE - $(days_ago)",
      "summarize": "SELECT product_id, SUM(quantity) AS total_sold FROM #get_sales GROUP BY product_id",
      "save": "INSERT INTO product_performance SELECT CURRENT_DATE, product_id, total_sold FROM #summarize"
    }',
    '{"order": ["get_sales", "summarize", "save"]}'
  );
  ASSERT pid IS NOT NULL, 'create_pipeline should return a pipeline_id';
  RAISE NOTICE 'PASS: pipeline created with id %', pid;
END $$;

-- ============================================================
-- Test 2: Execute pipeline
-- ============================================================
\echo '--- Test 2: execute_pipeline ---'
DO $$
DECLARE
  result JSONB;
  row_count INT;
BEGIN
  result := execute_pipeline('sales_summary', '{"days_ago": "30"}');

  ASSERT result->>'status' = 'completed', 'execution should complete';

  SELECT COUNT(*) INTO row_count FROM product_performance;
  ASSERT row_count = 3, 'should have 3 product rows, got ' || row_count;

  RAISE NOTICE 'PASS: pipeline executed, % product rows written', row_count;
END $$;

-- ============================================================
-- Test 3: Dry run
-- ============================================================
\echo '--- Test 3: dry_run ---'
DO $$
DECLARE
  result JSONB;
  resolved TEXT;
BEGIN
  result := execute_pipeline('sales_summary', '{"days_ago": "14"}', p_dry_run := true);

  ASSERT (result->>'dry_run')::boolean = true, 'should be a dry run';
  ASSERT result->'resolved_stages' IS NOT NULL, 'should have resolved_stages';

  resolved := result->'resolved_stages'->>'get_sales';
  ASSERT position('14' IN resolved) > 0, 'days_ago param should be resolved to 14';
  ASSERT position('$(' IN resolved) = 0, 'no unresolved params should remain';

  -- Verify no new execution was logged
  ASSERT (SELECT COUNT(*) FROM pipeline.executions WHERE pipeline_name = 'sales_summary') = 1,
    'dry run should not log an execution';

  RAISE NOTICE 'PASS: dry run resolved correctly, no execution logged';
END $$;

-- ============================================================
-- Test 4: Parameter validation catches unknown params
-- ============================================================
\echo '--- Test 4: parameter validation ---'
DO $$
DECLARE
  pid INT;
BEGIN
  pid := create_pipeline(
    'bad_params',
    'Pipeline with a typo in param reference',
    '{"limit_val": "10"}',
    '{
      "fetch": "SELECT * FROM recent_sales LIMIT $(limt_val)"
    }',
    '{"order": ["fetch"]}'
  );

  BEGIN
    PERFORM execute_pipeline('bad_params');
    RAISE EXCEPTION 'Should have raised an error for unknown param';
  EXCEPTION WHEN OTHERS THEN
    ASSERT SQLERRM LIKE '%Unknown parameter $(limt_val)%',
      'error should mention the unknown param, got: ' || SQLERRM;
    RAISE NOTICE 'PASS: unknown param "limt_val" caught';
  END;
END $$;

-- ============================================================
-- Test 5: Flow/stages consistency validation
-- ============================================================
\echo '--- Test 5: flow/stages consistency ---'
DO $$
BEGIN
  -- Stage in flow but not in stages
  BEGIN
    PERFORM create_pipeline(
      'bad_flow',
      'Mismatched flow',
      '{}',
      '{"a": "SELECT 1"}',
      '{"order": ["a", "b"]}'
    );
    RAISE EXCEPTION 'Should have raised for missing stage b';
  EXCEPTION WHEN OTHERS THEN
    ASSERT SQLERRM LIKE '%Stage "b" in flow.order not found%',
      'error should mention missing stage, got: ' || SQLERRM;
    RAISE NOTICE 'PASS: missing stage in flow caught';
  END;

  -- Stage defined but not in flow
  BEGIN
    PERFORM create_pipeline(
      'bad_flow2',
      'Orphan stage',
      '{}',
      '{"a": "SELECT 1", "b": "SELECT 2"}',
      '{"order": ["a"]}'
    );
    RAISE EXCEPTION 'Should have raised for orphan stage b';
  EXCEPTION WHEN OTHERS THEN
    ASSERT SQLERRM LIKE '%Stage "b" defined in stages but missing from flow.order%',
      'error should mention orphan stage, got: ' || SQLERRM;
    RAISE NOTICE 'PASS: orphan stage caught';
  END;
END $$;

-- ============================================================
-- Test 6: Stage reference word boundaries
-- ============================================================
\echo '--- Test 6: stage ref word boundaries ---'
DO $$
DECLARE
  pid INT;
  result JSONB;
  resolved TEXT;
BEGIN
  CREATE TABLE t_items (id INT, name TEXT);
  INSERT INTO t_items VALUES (1, 'x'), (2, 'y');

  pid := create_pipeline(
    'boundary_test',
    'Tests that #a does not clobber #ab',
    '{}',
    '{
      "a":  "SELECT id FROM t_items WHERE id = 1",
      "ab": "SELECT id FROM t_items WHERE id = 2",
      "c":  "SELECT * FROM #a UNION ALL SELECT * FROM #ab"
    }',
    '{"order": ["a", "ab", "c"]}'
  );

  result := execute_pipeline('boundary_test');
  ASSERT result->>'status' = 'completed', 'boundary pipeline should complete';

  RAISE NOTICE 'PASS: stage refs with shared prefixes resolved correctly';
  DROP TABLE t_items;
END $$;

-- ============================================================
-- Test 7: Pipeline status view
-- ============================================================
\echo '--- Test 7: pipeline.status view ---'
DO $$
DECLARE
  rec RECORD;
BEGIN
  SELECT * INTO rec FROM pipeline.status WHERE pipeline_name = 'sales_summary';
  ASSERT rec IS NOT NULL, 'sales_summary should appear in status view';
  ASSERT rec.total_executions >= 1, 'should have at least 1 execution';
  ASSERT rec.successful_executions >= 1, 'should have at least 1 successful execution';
  RAISE NOTICE 'PASS: status view shows % total, % successful executions',
    rec.total_executions, rec.successful_executions;
END $$;

-- ============================================================
-- Test 8: Stage executions view
-- ============================================================
\echo '--- Test 8: pipeline.stage_executions view ---'
DO $$
DECLARE
  stage_count INT;
BEGIN
  SELECT COUNT(*) INTO stage_count
  FROM pipeline.stage_executions
  WHERE pipeline_name = 'sales_summary';

  ASSERT stage_count = 3, 'should have 3 stage records, got ' || stage_count;
  RAISE NOTICE 'PASS: stage_executions has % stage records', stage_count;
END $$;

-- ============================================================
-- Test 9: Pipeline upsert (re-create overwrites)
-- ============================================================
\echo '--- Test 9: pipeline upsert ---'
DO $$
DECLARE
  pid INT;
  desc_text TEXT;
BEGIN
  pid := create_pipeline(
    'sales_summary',
    'Updated description',
    '{"days_ago": "7"}',
    '{
      "get_sales": "SELECT * FROM recent_sales WHERE sale_date > CURRENT_DATE - $(days_ago)",
      "summarize": "SELECT product_id, SUM(quantity) AS total_sold FROM #get_sales GROUP BY product_id",
      "save": "INSERT INTO product_performance SELECT CURRENT_DATE, product_id, total_sold FROM #summarize ON CONFLICT DO NOTHING"
    }',
    '{"order": ["get_sales", "summarize", "save"]}'
  );

  SELECT description INTO desc_text FROM pipeline.pipelines WHERE name = 'sales_summary';
  ASSERT desc_text = 'Updated description', 'description should be updated';
  RAISE NOTICE 'PASS: pipeline upsert works';
END $$;

-- ============================================================
-- Test 10: pipeline.runs view
-- ============================================================
\echo '--- Test 10: pipeline.runs view ---'
DO $$
DECLARE
  rec RECORD;
BEGIN
  SELECT * INTO rec FROM pipeline.runs WHERE pipeline_name = 'sales_summary' LIMIT 1;
  ASSERT FOUND, 'should have a run';
  ASSERT rec.status = 'completed', 'run should be completed';
  ASSERT rec.duration_ms IS NOT NULL, 'should have duration';
  ASSERT rec.total_records > 0, 'should have records, got ' || rec.total_records;
  RAISE NOTICE 'PASS: pipeline.runs shows % total records, %ms', rec.total_records, rec.duration_ms;
END $$;

-- ============================================================
-- Test 11: pipeline_history() function
-- ============================================================
\echo '--- Test 11: pipeline_history() ---'
DO $$
DECLARE
  rec RECORD;
  cnt INT;
BEGIN
  -- With pipeline name
  SELECT COUNT(*) INTO cnt FROM pipeline_history('sales_summary');
  ASSERT cnt >= 1, 'should have at least 1 run for sales_summary';

  -- Without pipeline name (all pipelines)
  SELECT COUNT(*) INTO cnt FROM pipeline_history();
  ASSERT cnt >= 2, 'should have runs across pipelines, got ' || cnt;

  -- With limit
  SELECT COUNT(*) INTO cnt FROM pipeline_history(NULL, 1);
  ASSERT cnt = 1, 'limit should work, got ' || cnt;

  RAISE NOTICE 'PASS: pipeline_history() works with and without filters';
END $$;

\echo ''
\echo '=== All tests passed ==='
