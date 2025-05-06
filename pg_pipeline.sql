-- pg_pipeline.sql - PostgreSQL pipeline extension
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
BEGIN
  parsed_parameters := p_parameters::JSONB;
  
  parsed_stages := p_stages::JSONB;
  
  parsed_flow := p_flow::JSONB;
  
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
  p_params TEXT DEFAULT '{}'
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
  stats_json JSONB;  -- Removed initialization here
  stage_stats JSONB := '{}'::JSONB;
BEGIN
  parsed_params := p_params::JSONB;

  SELECT * INTO pipeline FROM pipeline.pipelines WHERE name = p_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Pipeline not found: %', p_name;
  END IF;
  
  INSERT INTO pipeline.executions (pipeline_id, pipeline_name, parameters)
  VALUES (pipeline.pipeline_id, p_name, parsed_params)
  RETURNING execution_id INTO v_execution_id;
  
  SELECT array_agg(x.stage_name)
  INTO stage_order
  FROM jsonb_array_elements_text(pipeline.flow->'order') AS x(stage_name);
  
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
          -- Use ~> prefix for stage references
          stage_reference := '~>' || param_key;
          
          -- Create temporary table name (using TEMPORARY tables)
          temp_table_name := 'temp_stage_' || v_execution_id || '_' || param_key;
          
          -- Replace occurrences of the stage reference
          IF position(stage_reference IN stage_query) > 0 THEN
            stage_query := regexp_replace(stage_query, 
              stage_reference, 
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
          'records_out', records_count
        )
      );
      
      -- Fix: properly append to the stages array
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

-- Add convenience aliases in public schema if desired
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
  p_params TEXT DEFAULT '{}'
) RETURNS JSONB AS $$
  SELECT pipeline.execute_pipeline(p_name, p_params);
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
  (stage_data->stage_name->>'duration_ms')::numeric AS duration_ms,
  (stage_data->stage_name->>'records_out')::integer AS records_out,
  (stage_data->stage_name->>'started_at')::timestamp AS stage_started_at,
  (stage_data->stage_name->>'completed_at')::timestamp AS stage_completed_at
FROM stage_details
WHERE stage_name IS NOT NULL
ORDER BY started_at DESC, stage_started_at;



/**
Synthetic example 2
CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT, email TEXT);
CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT, amount NUMERIC, order_date DATE);
CREATE TABLE report (report_date DATE, customer TEXT, orders_count INT, total_spent NUMERIC);

INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com');
INSERT INTO orders VALUES 
  (1, 1, 100, CURRENT_DATE - 5),
  (2, 1, 200, CURRENT_DATE - 3),
  (3, 2, 150, CURRENT_DATE - 2),
  (4, 1, 75, CURRENT_DATE);

SELECT create_pipeline(
  'customer_report',                 -- Name of pipeline
  'Simple customer spending report', -- Description
                                     -- Define pipeline params
  '{
    "min_amount": "50",
    "days_ago": "7"
  }',
                                     -- Define your pipeline stages
  '{
    "get_orders": "SELECT c.name, o.amount, o.order_date FROM customers c JOIN orders o ON c.id = o.customer_id WHERE o.amount > $(min_amount) AND o.order_date > CURRENT_DATE - $(days_ago)::INTEGER",
    "aggregate": "SELECT CURRENT_DATE AS report_date, name, COUNT(*) AS orders_count, SUM(amount) AS total_spent FROM ~>get_orders GROUP BY name",
    "save": "INSERT INTO report SELECT * FROM ~>aggregate"
  }',
                                     -- Order stages
  '{"order": ["get_orders", "aggregate", "save"]}'
);

                                     -- Run pipeline
SELECT execute_pipeline('customer_report', '{}');

SELECT * FROM report;
SELECT execute_pipeline('customer_report', '{"min_amount": "150", "days_ago": "3"}');

*/


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

-- Create your pipelines (they are saved in pipeline.pipelines)
SELECT create_pipeline(
  'sales_summary',                 -- Name
  'Daily product sales summary',   -- Description
                                   -- Define params with default values 
  '{
    "days_ago": "7"
  }',
                                   -- Set your stages. Use params with $(param_name)
                                   -- Use previous stages with ~>stage_name
  '{
    "get_sales": "SELECT * FROM recent_sales WHERE sale_date > CURRENT_DATE - $(days_ago)::INTEGER",
    "summarize": "SELECT product_id, SUM(quantity) AS total_sold FROM ~>get_sales GROUP BY product_id",
    "save": "INSERT INTO product_performance SELECT CURRENT_DATE, product_id, total_sold FROM ~>summarize"
  }',
  '{"order": ["get_sales", "summarize", "save"]}' -- Slot stages in order
);

-- Execute your pipelines (with or w/out param overrides)
SELECT execute_pipeline('sales_summary', '{"days_ago": "6"}');

-- Get per stage execution metrics for free


*/
