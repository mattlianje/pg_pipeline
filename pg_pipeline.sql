CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.pipelines (
  pipeline_id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  description TEXT,
  parameters JSONB DEFAULT '{}'::JSONB,
  stages JSONB NOT NULL,
  flow JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline.executions (
  execution_id SERIAL PRIMARY KEY,
  pipeline_id INT REFERENCES pipeline.pipelines(pipeline_id),
  pipeline_name TEXT NOT NULL,
  parameters JSONB,
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP,
  status TEXT DEFAULT 'running',
  stats JSONB
);

CREATE OR REPLACE FUNCTION pipeline.create_pipeline(
  p_name TEXT,
  p_description TEXT,
  p_parameters TEXT,
  p_stages TEXT,
  p_flow TEXT
) RETURNS INT AS $$
DECLARE
  v_pipeline_id INT;
  parsed_parameters JSONB;
  parsed_stages JSONB;
  parsed_flow JSONB;
  flow_stage TEXT;
  stage_keys TEXT[];
  flow_stages TEXT[];
BEGIN
  parsed_parameters := p_parameters::JSONB;
  parsed_stages := p_stages::JSONB;
  parsed_flow := p_flow::JSONB;

  -- Validate: every stage in flow.order must exist in stages
  SELECT array_agg(k) INTO stage_keys FROM jsonb_object_keys(parsed_stages) AS k;
  SELECT array_agg(v) INTO flow_stages FROM jsonb_array_elements_text(parsed_flow->'order') AS v;

  IF stage_keys IS NULL OR array_length(stage_keys, 1) = 0 THEN
    RAISE EXCEPTION 'Pipeline must have at least one stage';
  END IF;

  IF flow_stages IS NULL OR array_length(flow_stages, 1) = 0 THEN
    RAISE EXCEPTION 'Pipeline flow.order must have at least one stage';
  END IF;

  FOREACH flow_stage IN ARRAY flow_stages
  LOOP
    IF NOT (flow_stage = ANY(stage_keys)) THEN
      RAISE EXCEPTION 'Stage "%" in flow.order not found in stages definition', flow_stage;
    END IF;
  END LOOP;

  FOREACH flow_stage IN ARRAY stage_keys
  LOOP
    IF NOT (flow_stage = ANY(flow_stages)) THEN
      RAISE EXCEPTION 'Stage "%" defined in stages but missing from flow.order', flow_stage;
    END IF;
  END LOOP;

  -- Create or replace the pipeline
  INSERT INTO pipeline.pipelines (name, description, parameters, stages, flow)
  VALUES (p_name, p_description, parsed_parameters, parsed_stages, parsed_flow)
  ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    parameters = EXCLUDED.parameters,
    stages = EXCLUDED.stages,
    flow = EXCLUDED.flow
  RETURNING pipeline_id INTO v_pipeline_id;
  
  RETURN v_pipeline_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pipeline.execute_pipeline(
  p_name TEXT,
  p_params TEXT DEFAULT '{}',
  p_dry_run BOOLEAN DEFAULT FALSE
) RETURNS JSONB AS $$
DECLARE
  parsed_params JSONB;
  pipeline RECORD;
  v_execution_id INT;
  stage_order TEXT[];
  stage_name TEXT;
  stage_query TEXT;
  temp_table_name TEXT;
  stage_reference TEXT;
  param_key TEXT;
  param_value TEXT;
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  stage_start_time TIMESTAMP;
  stage_end_time TIMESTAMP;
  records_count INT;
  stats_json JSONB;
  stage_stats JSONB := '{}'::JSONB;
  dry_run_stages JSONB := '{}'::JSONB;
  unresolved_match TEXT;
  all_param_keys TEXT[];
BEGIN
  parsed_params := p_params::JSONB;

  SELECT * INTO pipeline FROM pipeline.pipelines WHERE name = p_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Pipeline not found: %', p_name;
  END IF;

  -- Build merged parameter keys for validation
  SELECT array_agg(k) INTO all_param_keys FROM jsonb_object_keys(pipeline.parameters) AS k;
  IF all_param_keys IS NULL THEN
    all_param_keys := ARRAY[]::TEXT[];
  END IF;

  -- Validate: check all $(param) references resolve to known parameters
  FOR stage_name IN SELECT unnest(
    ARRAY(SELECT jsonb_array_elements_text(pipeline.flow->'order'))
  )
  LOOP
    stage_query := pipeline.stages ->> stage_name;
    FOR unresolved_match IN
      SELECT (regexp_matches(stage_query, '\$\(([a-zA-Z_][a-zA-Z0-9_]*)\)', 'g'))[1]
    LOOP
      IF NOT (unresolved_match = ANY(all_param_keys))
         AND NOT (parsed_params ? unresolved_match) THEN
        RAISE EXCEPTION 'Unknown parameter $(%): referenced in stage "%" but not defined in pipeline parameters or execution params',
          unresolved_match, stage_name;
      END IF;
    END LOOP;
  END LOOP;

  SELECT array_agg(x.stage_name)
  INTO stage_order
  FROM jsonb_array_elements_text(pipeline.flow->'order') AS x(stage_name);

  -- Dry run: resolve all queries and return without executing
  IF p_dry_run THEN
    dry_run_stages := jsonb_build_object();
    FOREACH stage_name IN ARRAY stage_order
    LOOP
      stage_query := pipeline.stages ->> stage_name;

      FOR param_key, param_value IN SELECT * FROM jsonb_each_text(pipeline.parameters)
      LOOP
        IF parsed_params ? param_key THEN
          param_value := parsed_params ->> param_key;
        END IF;
        stage_query := regexp_replace(stage_query,
          '\$\(' || param_key || '\)',
          param_value, 'g');
      END LOOP;

      FOR param_key IN SELECT unnest(stage_order)
      LOOP
        IF param_key != stage_name THEN
          stage_reference := '#' || param_key;
          temp_table_name := 'temp_stage_<exec_id>_' || param_key;
          IF position(stage_reference IN stage_query) > 0 THEN
            stage_query := regexp_replace(stage_query,
              '#' || param_key || '\M',
              temp_table_name, 'g');
          END IF;
        END IF;
      END LOOP;

      dry_run_stages := dry_run_stages || jsonb_build_object(stage_name, stage_query);
    END LOOP;

    RETURN jsonb_build_object(
      'dry_run', TRUE,
      'pipeline', p_name,
      'parameters', pipeline.parameters || parsed_params,
      'resolved_stages', dry_run_stages
    );
  END IF;

  INSERT INTO pipeline.executions (pipeline_id, pipeline_name, parameters)
  VALUES (pipeline.pipeline_id, p_name, parsed_params)
  RETURNING execution_id INTO v_execution_id;

  start_time := clock_timestamp();

  -- Initialize stats_json with empty stages array
  stats_json := jsonb_build_object('stages', jsonb_build_array());

  BEGIN
    -- Execute each stage in order
    FOREACH stage_name IN ARRAY stage_order
    LOOP
      stage_start_time := clock_timestamp();
      stage_query := pipeline.stages ->> stage_name;

      -- Replace parameter references $(param_name) with actual values
      FOR param_key, param_value IN SELECT * FROM jsonb_each_text(pipeline.parameters)
      LOOP
        -- Use param or default
        IF parsed_params ? param_key THEN
          param_value := parsed_params ->> param_key;
        END IF;

        -- Replace parameter in query
        stage_query := regexp_replace(stage_query,
          '\$\(' || param_key || '\)',
          param_value,
          'g');
      END LOOP;

      -- Replaces references to previous stages with the actual temp table names
      FOR param_key IN SELECT unnest(stage_order)
      LOOP
        -- Only replace references to stages that already executed
        IF param_key != stage_name THEN
          -- Use # prefix for stage references
          stage_reference := '#' || param_key;

          -- Create temporary table name (using TEMPORARY tables)
          temp_table_name := 'temp_stage_' || v_execution_id || '_' || param_key;

          -- Replace occurrences of the stage reference (word boundary to avoid partial matches)
          IF position(stage_reference IN stage_query) > 0 THEN
            stage_query := regexp_replace(stage_query,
              '#' || param_key || '\M',
              temp_table_name,
              'g');
          END IF;
        END IF;
      END LOOP;

      temp_table_name := 'temp_stage_' || v_execution_id || '_' || stage_name;

      IF stage_query ~* '^INSERT|^UPDATE|^DELETE' THEN
        EXECUTE stage_query;
        GET DIAGNOSTICS records_count = ROW_COUNT;

        EXECUTE 'CREATE TEMPORARY TABLE ' || temp_table_name || ' AS SELECT 1 AS execution_record LIMIT ' || records_count;
      ELSE
        EXECUTE 'CREATE TEMPORARY TABLE ' || temp_table_name || ' AS ' || stage_query;
        EXECUTE 'SELECT COUNT(*) FROM ' || temp_table_name INTO records_count;
      END IF;

      stage_end_time := clock_timestamp();

      stage_stats := jsonb_build_object(
        stage_name, jsonb_build_object(
          'started_at', stage_start_time,
          'completed_at', stage_end_time,
          'duration_ms', EXTRACT(EPOCH FROM (stage_end_time - stage_start_time)) * 1000,
          'records_out', records_count,
          'type', CASE WHEN stage_query ~* '^INSERT|^UPDATE|^DELETE' THEN 'write' ELSE 'read' END
        )
      );

      stats_json := jsonb_set(
        stats_json,
        '{stages}',
        (stats_json->'stages') || jsonb_build_array(stage_stats)
      );
    END LOOP;

    end_time := clock_timestamp();

    stats_json := stats_json || jsonb_build_object(
      'total_duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000
    );

    UPDATE pipeline.executions
    SET
      completed_at = end_time,
      status = 'completed',
      stats = stats_json
    WHERE execution_id = v_execution_id;

    -- Return execution info
    RETURN jsonb_build_object(
      'execution_id', v_execution_id,
      'status', 'completed',
      'stats', stats_json
    );

  EXCEPTION WHEN OTHERS THEN
    UPDATE pipeline.executions
    SET
      completed_at = clock_timestamp(),
      status = 'failed',
      stats = jsonb_build_object(
        'error', SQLERRM,
        'stage', stage_name,
        'stages', stats_json->'stages'
      )
    WHERE execution_id = v_execution_id;

    RAISE;
  END;
END;
$$ LANGUAGE plpgsql;

-- Status view
CREATE OR REPLACE VIEW pipeline.status AS
SELECT 
  p.name AS pipeline_name,
  p.description,
  COUNT(e.execution_id) AS total_executions,
  COUNT(CASE WHEN e.status = 'completed' THEN 1 END) AS successful_executions,
  COUNT(CASE WHEN e.status = 'failed' THEN 1 END) AS failed_executions,
  MAX(e.started_at) AS last_execution,
  MAX(CASE WHEN e.status = 'completed' THEN e.started_at END) AS last_successful_execution
FROM pipeline.pipelines p
LEFT JOIN pipeline.executions e ON p.pipeline_id = e.pipeline_id
GROUP BY p.pipeline_id, p.name, p.description
ORDER BY p.name;

-- Add convenience aliases in public schema
CREATE OR REPLACE FUNCTION create_pipeline(
  p_name TEXT,
  p_description TEXT,
  p_parameters TEXT,
  p_stages TEXT,
  p_flow TEXT
) RETURNS INT AS $$
  SELECT pipeline.create_pipeline(p_name, p_description, p_parameters, p_stages, p_flow);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION execute_pipeline(
  p_name TEXT,
  p_params TEXT DEFAULT '{}',
  p_dry_run BOOLEAN DEFAULT FALSE
) RETURNS JSONB AS $$
  SELECT pipeline.execute_pipeline(p_name, p_params, p_dry_run);
$$ LANGUAGE SQL;

-- Per stage view
CREATE OR REPLACE VIEW pipeline.stage_executions AS
WITH unnested_stages AS (
  SELECT 
    execution_id,
    pipeline_id,
    pipeline_name,
    started_at,
    completed_at,
    status,
    jsonb_array_elements(stats->'stages') AS stage_data
  FROM pipeline.executions
),
stage_details AS (
  SELECT
    execution_id,
    pipeline_id,
    pipeline_name,
    started_at,
    completed_at,
    status,
    stage_data,
    CASE
      WHEN jsonb_typeof(stage_data) = 'object' THEN 
        (SELECT key FROM jsonb_each(stage_data) LIMIT 1)
      ELSE NULL
    END AS stage_name
  FROM unnested_stages
  WHERE stage_data IS NOT NULL AND jsonb_typeof(stage_data) = 'object'
)
SELECT
  execution_id,
  pipeline_id,
  pipeline_name,
  started_at,
  completed_at,
  status,
  stage_name,
  stage_data->stage_name->>'type' AS type,
  (stage_data->stage_name->>'duration_ms')::numeric AS duration_ms,
  (stage_data->stage_name->>'records_out')::integer AS records_out,
  (stage_data->stage_name->>'started_at')::timestamp AS stage_started_at,
  (stage_data->stage_name->>'completed_at')::timestamp AS stage_completed_at
FROM stage_details
WHERE stage_name IS NOT NULL
ORDER BY started_at DESC, stage_started_at;

-- List all pipelines
CREATE OR REPLACE VIEW pipeline.list AS
SELECT
  p.name,
  p.description,
  p.parameters,
  jsonb_array_length(p.flow->'order') AS num_stages,
  p.flow->'order' AS stage_order,
  p.created_at,
  s.total_executions,
  s.last_execution
FROM pipeline.pipelines p
LEFT JOIN pipeline.status s ON s.pipeline_name = p.name
ORDER BY p.name;

-- Run-level view (one row per execution)
CREATE OR REPLACE VIEW pipeline.runs AS
SELECT
  e.execution_id,
  e.pipeline_name,
  e.status,
  e.parameters,
  e.started_at,
  e.completed_at,
  ROUND((stats->>'total_duration_ms')::numeric) AS duration_ms,
  (SELECT COALESCE(SUM(((v->(SELECT key FROM jsonb_each(v) LIMIT 1))->>'records_out')::int), 0)
   FROM jsonb_array_elements(e.stats->'stages') AS v
   WHERE (v->(SELECT key FROM jsonb_each(v) LIMIT 1))->>'type' = 'read') AS records_read,
  (SELECT COALESCE(SUM(((v->(SELECT key FROM jsonb_each(v) LIMIT 1))->>'records_out')::int), 0)
   FROM jsonb_array_elements(e.stats->'stages') AS v
   WHERE (v->(SELECT key FROM jsonb_each(v) LIMIT 1))->>'type' = 'write') AS records_written,
  stats->>'error' AS error
FROM pipeline.executions e
ORDER BY e.started_at DESC;

-- Convenience: query recent runs for a pipeline
CREATE OR REPLACE FUNCTION pipeline.history(
  p_name TEXT DEFAULT NULL,
  p_limit INT DEFAULT 10
) RETURNS TABLE (
  execution_id INT,
  pipeline_name TEXT,
  status TEXT,
  parameters JSONB,
  started_at TIMESTAMP,
  duration_ms NUMERIC,
  records_read BIGINT,
  records_written BIGINT,
  error TEXT
) AS $$
  SELECT execution_id, pipeline_name, status, parameters, started_at, duration_ms, records_read, records_written, error
  FROM pipeline.runs
  WHERE (p_name IS NULL OR pipeline.runs.pipeline_name = p_name)
  ORDER BY started_at DESC
  LIMIT p_limit;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION pipeline_history(
  p_name TEXT DEFAULT NULL,
  p_limit INT DEFAULT 10
) RETURNS TABLE (
  execution_id INT,
  pipeline_name TEXT,
  status TEXT,
  parameters JSONB,
  started_at TIMESTAMP,
  duration_ms NUMERIC,
  records_read BIGINT,
  records_written BIGINT,
  error TEXT
) AS $$
  SELECT * FROM pipeline.history(p_name, p_limit);
$$ LANGUAGE SQL;

/***
Example number 2:
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

-- Step 1: Create a pipeline with `create_pipeline`
SELECT create_pipeline(
  'sales_summary',                 -- Set pipeline name
  'Daily product sales summary',   -- Add a description
  '{"days_ago": "7"}',             -- Create params w/ default values

  -- Set your stages. Use params with $(param_name), previous stages with #stage_name
  '{
    "get_sales": "SELECT * FROM recent_sales WHERE sale_date > CURRENT_DATE - $(days_ago)",
    "summarize": "SELECT product_id, SUM(quantity) AS total_sold FROM #get_sales GROUP BY product_id",
    "save": "INSERT INTO product_performance SELECT CURRENT_DATE, product_id, total_sold FROM #summarize"
  }',
  '{"order": ["get_sales", "summarize", "save"]}' -- Sequence your pipeline stages
);

-- Step 2: Run your pipeline w/ `execute_pipeline`
SELECT execute_pipeline('sales_summary', '{"days_ago": "6"}');

-- Query past runs, row counts, etc in pipeline.stage_exeuctions
SELECT pipeline_name, execution_id, started_at, stage_name, duration_ms, records_out
FROM pipeline.stage_executions ORDER BY execution_id DESC LIMIT 3;

*/
