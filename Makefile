.PHONY: setup pipeline test api up down smoke incremental compact-demo load-test ci clean

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

pipeline:
	.venv/bin/python -m pipeline.run --input data/raw --output data/lake --db data/serving.db

test:
	PYTHONPATH=. .venv/bin/pytest -q

api:
	API_KEY=local-dev-key SERVING_DB=data/serving.db .venv/bin/uvicorn api.main:app --reload

up:
	docker compose up -d --build

down:
	docker compose down

smoke:
	curl --fail http://localhost:8000/v1/health
	curl --fail http://localhost:9090/-/healthy
	curl --fail http://localhost:3000/api/health

incremental:
	docker compose run --rm pipeline

compact-demo:
	docker compose run --rm pipeline python -m pipeline.demo_small_files

load-test:
	docker compose run --rm pipeline python scripts/load_test.py --base-url http://api:8000 --requests 100 --concurrency 10

ci: test compact-demo smoke

clean:
	rm -rf data/lake data/state data/quarantine data/serving.db
