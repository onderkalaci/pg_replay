EXTENSION = pg_replay
EXTVERSIONS = 1.0

DATA_built = $(foreach v,$(EXTVERSIONS),$(EXTENSION)--$(v).sql)
DATA = $(wildcard $(EXTENSION)--*--*.sql)

MODULES = pg_replay

REGRESS = setup $(filter-out setup,$(patsubst sql/%.sql,%,$(sort $(wildcard sql/*.sql))))

PG_CONFIG ?= pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(EXTENSION)--1.0.sql: $(EXTENSION).sql
	cat $^ > $@