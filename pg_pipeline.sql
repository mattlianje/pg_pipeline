-- pg_pipeline.sql - PostgreSQL pipeline extension

CREATE SCHEMA IF NOT EXISTS pg_pipeline;

CREATE TABLE IF NOT EXISTS pg_pipeline.pipelines (
  pipeline_id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  description TEXT,
  parameters JSONB DEFAULT '{}'::JSONB,
  stages JSONB NOT NULL,
  flow JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pg_pipeline.executions (
  execution_id SERIAL PRIMARY KEY,
  pipeline_id INT REFERENCES pg_pipeline.pipelines(pipeline_id),
  pipeline_name TEXT NOT NULL,
  parameters JSONB,
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP,
  status TEXT DEFAULT 'running',
  stats JSONB
);

CREATE OR REPLACE FUNCTION pg_pipeline.create_pipeline(
  p_name TEXT,
  p_description TEXT,
  p_parameters TEXT,
  p_stages TEXT,
  p_flow TEXT
) RETURNS INT AS $$
DECLARE
  pipeline_id INT;
  parsed_parameters JSONB;
  parsed_stages JSONB;
  parsed_flow JSONB;
BEGIN
  parsed_parameters := p_parameters::JSONB;
  
  parsed_stages := p_stages::JSONB;
  
  parsed_flow := p_flow::JSONB;
  
  -- Create or replace the pipeline
  INSERT INTO pg_pipeline.pipelines (name, description, parameters, stages, flow)
  VALUES (p_name, p_description, parsed_parameters, parsed_stages, parsed_flow)
  ON CONFLICT (name) DO UPDATE SET
    description = EXCLUDED.description,
    parameters = EXCLUDED.parameters,
    stages = EXCLUDED.stages,
    flow = EXCLUDED.flow
  RETURNING pipeline_id INTO pipeline_id;
  
  RETURN pipeline_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pg_pipeline.execute_pipeline(
  p_name TEXT,
  p_params TEXT DEFAULT '{}'
) RETURNS JSONB AS $$
DECLARE
  parsed_params JSONB;
  pipeline RECORD;
  execution_id INT;
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
  stats_json JSONB := '{}'::JSONB;
  stage_stats JSONB := '{}'::JSONB;
BEGIN
  parsed_params := p_params::JSONB;

  SELECT * INTO pipeline FROM pg_pipeline.pipelines WHERE name = p_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Pipeline not found: %', p_name;
  END IF;
  
  INSERT INTO pg_pipeline.executions (pipeline_id, pipeline_name, parameters)
  VALUES (pipeline.pipeline_id, p_name, parsed_params)
  RETURNING execution_id INTO execution_id;
  
  -- Get execution order from flow
  SELECT array_agg(jsonb_array_elements_text(pipeline.flow->'order')) INTO stage_order;
  
  start_time := clock_timestamp();
  
  BEGIN
    -- Execute each stage in order
    FOREACH stage_name IN ARRAY stage_order
    LOOP
      stage_start_time := clock_timestamp();
      stage_query := pipeline.stages ->> stage_name;
      
      -- Replace parameter references $(param_name) with actual values
      FOR param_key, param_value IN SELECT * FROM jsonb_each_text(pipeline.parameters)
      LOOP
        -- Use provided parameter value if available, otherwise use default
        IF parsed_params ? param_key THEN
          param_value := parsed_params ->> param_key;
        END IF;
        
        -- Replace parameter in query
        stage_query := regexp_replace(stage_query, 
          '\$\(' || param_key || '\)', 
          param_value, 
          'g');
      END LOOP;
      
      -- Replace references to previous stages with the actual temp table names
      FOR param_key IN SELECT unnest(stage_order)
      LOOP
        -- Only replace references to stages that already executed
        IF param_key != stage_name THEN
          -- Use ~> prefix for stage references
          stage_reference := '~>' || param_key;
          
          -- Create temporary table name (using TEMPORARY tables)
          temp_table_name := 'temp_stage_' || execution_id || '_' || param_key;
          
          -- Replace occurrences of the stage reference
          IF position(stage_reference IN stage_query) > 0 THEN
            stage_query := regexp_replace(stage_query, 
              stage_reference, 
              temp_table_name, 
              'g');
          END IF;
        END IF;
      END LOOP;
      
      -- Create temp table name for current stage
      temp_table_name := 'temp_stage_' || execution_id || '_' || stage_name;
      
      -- Execute the stage query
      IF stage_query ~* '^INSERT|^UPDATE|^DELETE' THEN
        -- For DML operations, execute directly and get row count
        EXECUTE stage_query;
        GET DIAGNOSTICS records_count = ROW_COUNT;
        
        -- Also create a temporary table with the affected rows for potential downstream use
        EXECUTE 'CREATE TEMPORARY TABLE ' || temp_table_name || ' AS SELECT 1 AS execution_record LIMIT ' || records_count;
      ELSE
        -- For queries, create a temporary table with the results
        EXECUTE 'CREATE TEMPORARY TABLE ' || temp_table_name || ' AS ' || stage_query;
        EXECUTE 'SELECT COUNT(*) FROM ' || temp_table_name INTO records_count;
      END IF;
      
      stage_end_time := clock_timestamp();
      
      stage_stats := jsonb_build_object(
        stage_name, jsonb_build_object(
          'started_at', stage_start_time,
          'completed_at', stage_end_time,
          'duration_ms', EXTRACT(EPOCH FROM (stage_end_time - stage_start_time)) * 1000,
          'records_out', records_count
        )
      );
      
      stats_json := stats_json || jsonb_build_object('stages', stats_json->'stages' || stage_stats);
    END LOOP;
    
    end_time := clock_timestamp();
    
    stats_json := stats_json || jsonb_build_object(
      'total_duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000
    );
    
    UPDATE pg_pipeline.executions
    SET 
      completed_at = end_time,
      status = 'completed',
      stats = stats_json
    WHERE execution_id = execution_id;
    
    -- Return execution info
    RETURN jsonb_build_object(
      'execution_id', execution_id,
      'status', 'completed',
      'stats', stats_json
    );
    
  EXCEPTION WHEN OTHERS THEN
    UPDATE pg_pipeline.executions
    SET 
      completed_at = clock_timestamp(),
      status = 'failed',
      stats = jsonb_build_object(
        'error', SQLERRM,
        'stage', stage_name,
        'stages', stats_json->'stages'
      )
    WHERE execution_id = execution_id;
    
    RAISE;
  END;
END;
$$ LANGUAGE plpgsql;

-- Status view
CREATE OR REPLACE VIEW pg_pipeline.status AS
SELECT 
  p.name AS pipeline_name,
  p.description,
  COUNT(e.execution_id) AS total_executions,
  COUNT(CASE WHEN e.status = 'completed' THEN 1 END) AS successful_executions,
  COUNT(CASE WHEN e.status = 'failed' THEN 1 END) AS failed_executions,
  MAX(e.started_at) AS last_execution,
  MAX(CASE WHEN e.status = 'completed' THEN e.started_at END) AS last_successful_execution
FROM pg_pipeline.pipelines p
LEFT JOIN pg_pipeline.executions e ON p.pipeline_id = e.pipeline_id
GROUP BY p.pipeline_id, p.name, p.description
ORDER BY p.name;

-- Add convenience aliases in public schema if desired
CREATE OR REPLACE FUNCTION create_pipeline(
  p_name TEXT,
  p_description TEXT,
  p_parameters TEXT,
  p_stages TEXT,
  p_flow TEXT
) RETURNS INT AS $$
  SELECT pg_pipeline.create_pipeline(p_name, p_description, p_parameters, p_stages, p_flow);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION execute_pipeline(
  p_name TEXT,
  p_params TEXT DEFAULT '{}'
) RETURNS JSONB AS $$
  SELECT pg_pipeline.execute_pipeline(p_name, p_params);
$$ LANGUAGE SQL;

-- Example usage (commented out)
/*
SELECT create_pipeline(
  'etl_customer_analytics',
  'Pipeline for customer analytics',
  '{
    "start_date": "CURRENT_DATE - INTERVAL ''30 days''",
    "customer_segment": "all",
    "vip_threshold": "1000"
  }',
  '{
    "extract_orders": "SELECT * FROM raw_orders WHERE order_date >= $(start_date) AND ($(customer_segment) = ''all'' OR customer_segment = $(customer_segment))",
    
    "transform_orders": "SELECT customer_id, COUNT(*) as order_count, SUM(order_total) as total_spent FROM ~>extract_orders GROUP BY customer_id",
    
    "enrich_customers": "SELECT c.customer_id, c.name, c.email, t.order_count, t.total_spent, CASE WHEN t.total_spent > $(vip_threshold) THEN ''VIP'' ELSE ''Regular'' END as segment FROM customers c JOIN ~>transform_orders t ON c.customer_id = t.customer_id",
    
    "load_analytics": "INSERT INTO customer_analytics SELECT * FROM ~>enrich_customers"
  }',
  '{
    "order": ["extract_orders", "transform_orders", "enrich_customers", "load_analytics"]
  }'
);

-- Run the pipeline
SELECT execute_pipeline(
  'etl_customer_analytics',
  '{"start_date": "CURRENT_DATE - INTERVAL ''90 days''", "vip_threshold": "500"}'
);


SELECT create_pipeline(
 'customer_metrics',
 'Customer behavior tracking',
 '{"period": "7"}',
 '{
   "active_users": "SELECT user_id, COUNT(*) AS visits FROM logins WHERE date > current_date - $(period)::int GROUP BY user_id",
   "purchases": "SELECT user_id, SUM(amount) AS spent FROM orders WHERE date > current_date - $(period)::int GROUP BY user_id",
   "combined": "SELECT a.user_id, a.visits, COALESCE(p.spent, 0) AS spent FROM ~>active_users a LEFT JOIN ~>purchases p ON a.user_id = p.user_id",
   "save_metrics": "INSERT INTO metrics_history (date, metrics) SELECT current_date, jsonb_build_object(''users'', COUNT(*), ''total_spent'', SUM(spent)) FROM ~>combined RETURNING 1"
 }',
 '{"order": ["active_users", "purchases", "combined", "save_metrics"]}'
);

-- Run with different time periods
SELECT execute_pipeline('customer_metrics', '{"period": "1"}');  -- daily
SELECT execute_pipeline('customer_metrics', '{"period": "7"}');  -- weekly
SELECT execute_pipeline('customer_metrics', '{"period": "30"}'); -- monthly

-- Get rich execution stats instead of setting up complex monitoring
SELECT 
 pipeline_name,
 started_at,
 completed_at,
 (stats->'total_duration_ms')::numeric/1000 AS duration_seconds,
 stats->'stages'->'active_users'->'records_out' AS active_users_count,
 stats->'stages'->'purchases'->'records_out' AS purchase_count
FROM pg_pipeline.executions
WHERE pipeline_name = 'customer_metrics'
ORDER BY started_at DESC
LIMIT 10;
*/
