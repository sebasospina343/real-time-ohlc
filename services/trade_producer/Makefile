build:
	docker build -t trade-producer .

run: build
	docker run --network=redpanda_network --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 trade-producer

dev:
	KAFKA_BROKER_ADDRESS=localhost:19092 LIVE_OR_HISTORICAL=live poetry run python src/main.py

dev-historical:
	KAFKA_BROKER_ADDRESS=localhost:19092 LIVE_OR_HISTORICAL=historical LAST_N_DAYS=3 poetry run python src/main.py

lint:
	ruff check --fix

format:
	ruff format .