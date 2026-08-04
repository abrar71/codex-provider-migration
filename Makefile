PYTHON ?= python3

.PHONY: check test

check:
	$(PYTHON) -B -m unittest discover -s tests -v

test:
	$(PYTHON) -B -m unittest discover -s tests -v
