build:
	docker build -t trade-producer .

run: build
	docker run --network=redpanda_network trade-producer

lint:
	ruff check --fix

format:
	ruff format .