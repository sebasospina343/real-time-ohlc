build:
	docker build -t trade-producer .

run: build
	docker run --network=redpanda_network --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 trade-producer

dev:
	 source .live.env && poetry run python src/main.py

dev-historical:
	source .historical.env && poetry run python src/main.py

lint:
	ruff check --fix

format:
	ruff format .