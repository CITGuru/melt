# Melt bench harness — `make bench` produces a queries-per-dollar delta.
#
# Required env (real mode):
#   SNOWFLAKE_ACCOUNT  SNOWFLAKE_USER  SNOWFLAKE_PASSWORD
#   SNOWFLAKE_DATABASE  SNOWFLAKE_SCHEMA  (defaults: ANALYTICS / PUBLIC)
#
# Optional:
#   MELT_HOST=127.0.0.1 MELT_PORT=8443 MELT_PROTOCOL=http
#   BENCH_CREDIT_RATE=3.00          $/credit override
#
# Synthetic (no Snowflake account needed): `make bench-synthetic`.

PYTHON ?= python3
PIP    ?= pip3
RUNS   ?= 100
CONC   ?= 4
OUT    ?= results-$(shell date -u +%Y%m%dT%H%M%SZ).json

.PHONY: help install bench bench-synthetic routes-fixture up down logs clean

help:
	@echo "make install            - pip install requirements.txt"
	@echo "make up                 - docker compose up --build -d (proxy + deps)"
	@echo "make down               - docker compose down --volumes"
	@echo "make bench              - run real bench, write to $(OUT)"
	@echo "make bench-synthetic    - run no-credentials bench, write a fixture"
	@echo "make routes-fixture     - regenerate fixtures/routes.json from workload.toml"
	@echo "make logs               - tail melt logs"
	@echo "make clean              - rm results-*.json (keeps fixtures/)"

install:
	$(PIP) install -r requirements.txt

up:
	docker compose -f ../../docker-compose.yml up --build -d
	@echo "wait for melt readiness:"
	@for i in $$(seq 1 30); do \
		if curl -fsS http://localhost:9090/readyz >/dev/null 2>&1; then \
			echo "  ready."; exit 0; \
		fi; sleep 2; \
	done; echo "  not ready after 60s — check 'make logs'"; exit 1

down:
	docker compose -f ../../docker-compose.yml down --volumes

logs:
	docker compose -f ../../docker-compose.yml logs -f melt | grep -E "statement_complete|route="

bench:
	$(PYTHON) run_bench.py --runs $(RUNS) --concurrency $(CONC) --out $(OUT)

bench-synthetic:
	mkdir -p fixtures
	$(PYTHON) run_bench.py --synthetic --runs $(RUNS) --concurrency $(CONC) \
		--out fixtures/results-synthetic.json

# Regenerate the routing golden fixture. Run after an intentional router
# change; the CI test in `crates/melt-router/tests/bench_routes_fixture.rs`
# fails on any drift from this file. Anchored to POWA-163 / §A6 of the
# launch-checklist.
routes-fixture:
	$(PYTHON) fixtures/regen_routes.py

clean:
	@find . -maxdepth 1 -name "results-*.json" -print -delete
