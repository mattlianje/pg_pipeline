PG_DB   ?= pg_pipeline_test
PG_ADMIN?= postgres
PSQL    ?= psql

.PHONY: test install clean sandbox

test:
	@echo "Creating test database $(PG_DB)..."
	@$(PSQL) -d $(PG_ADMIN) -c "DROP DATABASE IF EXISTS $(PG_DB);" 2>/dev/null || true
	@$(PSQL) -d $(PG_ADMIN) -c "CREATE DATABASE $(PG_DB);"
	@echo ""
	@$(PSQL) -d $(PG_DB) -f test.sql && \
		(echo ""; $(PSQL) -d $(PG_ADMIN) -c "DROP DATABASE $(PG_DB);") || \
		(echo ""; echo "FAIL: tests did not pass"; $(PSQL) -d $(PG_ADMIN) -c "DROP DATABASE $(PG_DB);" 2>/dev/null; exit 1)

install:
	$(PSQL) -f pg_pipeline.sql

sandbox:
	@$(PSQL) -d $(PG_ADMIN) -c "DROP DATABASE IF EXISTS $(PG_DB);" 2>/dev/null || true
	@$(PSQL) -d $(PG_ADMIN) -c "CREATE DATABASE $(PG_DB);"
	@$(PSQL) -d $(PG_DB) -f pg_pipeline.sql -q
	@echo "pg_pipeline installed. Dropping into $(PG_DB)...\n"
	@$(PSQL) -d $(PG_DB)

clean:
	@$(PSQL) -d $(PG_ADMIN) -c "DROP DATABASE IF EXISTS $(PG_DB);" 2>/dev/null || true
	@echo "Cleaned up."
