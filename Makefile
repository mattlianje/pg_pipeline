REGRESS = pg_pipeline_test
REGRESS_OPTS = --inputdir=test

installcheck:
	$(pg_regress_installcheck) $(REGRESS_OPTS) $(REGRESS)
