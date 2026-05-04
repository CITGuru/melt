# Melt agent-cost demo — `make demo` is the only command you need.
#
# Synthetic mode (default): no Snowflake account, no proxy required.
# Seed mode: requires POWA-92 (`melt sessions seed`) to be merged. Once that
# lands, `make demo MODE=seed` will time real proxy round-trips.

PYTHON ?= python3

.PHONY: help demo seed clean

help:
	@echo "make demo            - run 200-query agent workload, print \$$/query table"
	@echo "make demo MODE=seed  - same, but go through local seed-mode proxy (POWA-92)"
	@echo "make clean           - rm __pycache__"

MODE ?= synthetic

demo:
	@$(PYTHON) demo.py --mode $(MODE)

seed:
	@$(PYTHON) demo.py --mode seed

clean:
	@rm -rf __pycache__
