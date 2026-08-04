PYTHON ?= python3
IMAGE ?= codex-provider-migration:local

.PHONY: check test docker-build docker-check

check:
	$(PYTHON) -B -m unittest discover -s tests -v

test:
	$(PYTHON) -B -m unittest discover -s tests -v

docker-build:
	docker build --tag $(IMAGE) .

docker-check: docker-build
	docker run --rm --network none $(IMAGE) migrate --version
	docker run --rm --network none $(IMAGE) verify --version
	docker run --rm --network none $(IMAGE) restore --version
