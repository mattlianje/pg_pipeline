# pg_pipeline Demo
# Full lifecycle: install → define → execute → observe

Output pix/demo.gif
Set FontSize 20
Set Width 1800
Set Height 900
Set Theme "Catppuccin Mocha"
Set TypingSpeed 70ms
Set Padding 10
Set Framerate 60

# ── Setup: create DB, install pg_pipeline, seed demo tables + data ──
Hide
Type "psql -d postgres -c 'DROP DATABASE IF EXISTS pg_pipeline_demo;'"
Enter
Sleep 2s
Type "psql -d postgres -c 'CREATE DATABASE pg_pipeline_demo;'"
Enter
Sleep 2s
Type "psql -d pg_pipeline_demo"
Enter
Sleep 2s
Type "\pset pager off"
Enter
Sleep 500ms
Type "\i pg_pipeline.sql"
Enter
Sleep 2s
Type "\i demo.sql"
Enter
Sleep 3s
Show
Ctrl+L
Sleep 500ms

# ── 1. Create a pipeline ──
Type "-- Define a 2-stage pipeline with a parameterized lookback window"
Enter
Sleep 500ms
Set TypingSpeed 30ms
Type "SELECT create_pipeline("
Enter
Type "  'revenue_by_product',"
Enter
Type "  'Revenue breakdown by product',"
Enter
Type `  '{"days": "7"}',`
Enter
Type `  '{"recent_orders": "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL ''$(days) days''",`
Enter
Type `    "by_product":     "SELECT product, COUNT(*) AS num_orders, SUM(amount) AS revenue FROM #recent_orders GROUP BY product ORDER BY revenue DESC"}',`
Enter
Type `  '{"order": ["recent_orders", "by_product"]}'`
Enter
Type ");"
Enter
Sleep 3s
Set TypingSpeed 70ms

# ── 2. Execute it ──
Type "-- Run it with days=7"
Enter
Sleep 500ms
Type `SELECT jsonb_pretty(execute_pipeline('revenue_by_product', '{"days": "7"}'));`
Enter
Sleep 4s

# ── 3. Same pipeline, different params ──
Type "-- Same pipeline, wider window: days=30"
Enter
Sleep 500ms
Type `SELECT jsonb_pretty(execute_pipeline('revenue_by_product', '{"days": "30"}'));`
Enter
Sleep 4s

# ── 4. Observability: check all runs ──
Type "-- All pipeline runs with timing and row counts"
Enter
Sleep 500ms
Type "SELECT pipeline_name, status, started_at, duration_ms, records_read, records_written FROM pipeline.runs;"
Enter
Sleep 5s

# ── 5. Pipeline health at a glance ──
Type "-- Pipeline health at a glance"
Enter
Sleep 500ms
Type "SELECT pipeline_name, total_executions, successful_executions, failed_executions FROM pipeline.status;"
Enter
Sleep 5s

Hide
Type "\q"
Enter
