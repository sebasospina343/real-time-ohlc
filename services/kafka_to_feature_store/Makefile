build:
	docker build -t kafka-to-feature-store .

run: build
	docker run --network=redpanda_network kafka-to-feature-store

lint:
	ruff check --fix

format:
	ruff format .