build:
	docker build -t trade-to-ohlc .

run: build
	docker run --network=redpanda_network trade-to-ohlc

lint:
	ruff check --fix

format:
	ruff format .