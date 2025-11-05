build:
	docker build -t trade-to-ohlc .

run: build
	docker run --network=redpanda_network --env KAFKA_BROKER_ADDRESS=redpanda-0:9092 --env OHLC_WINDOWS_SECONDS=10 trade-to-ohlc

dev:
	KAFKA_BROKER_ADDRESS=localhost:19092 OHLC_WINDOWS_SECONDS=60 poetry run python src/main.py

lint:
	ruff check --fix

format:
	ruff format .