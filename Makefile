EXTENSION = pg_pipeline
DATA = pg_pipeline--0.1.sql
MODULES = pg_pipeline
REGRESS = pg_pipeline_test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
