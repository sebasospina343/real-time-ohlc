build:
	docker build -t kafka-to-feature-store .

run: build
	docker run --network=redpanda_network kafka-to-feature-store

dev:
	 KAFKA_BROKER_ADDRESS=localhost:19092 source .live.env && poetry run python src/main.py

dev-historical:
	 KAFKA_BROKER_ADDRESS=localhost:19092 \
	 source .historical.env && poetry run python src/main.py

lint:
	ruff check --fix

format:
	ruff format .