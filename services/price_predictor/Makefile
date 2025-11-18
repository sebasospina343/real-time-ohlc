train:
	 source .live.env && poetry run python src/training.py

predict:
	 source .live.env && poetry run python src/predictor.py

api:
	 source .live.env && poetry run python src/api.py

request:
	curl -X POST http://localhost:5000/predict -H "Content-Type: application/json"

copy-tools:
	cp -r ../../tools2 .

build: copy-tools
	docker build -t price-predictor-api .

stop:
	-docker ps -q --filter ancestor=price-predictor-api | xargs docker stop 2>/dev/null || true
	-docker ps --format "{{.ID}}" | while read id; do \
		docker port $$id 2>/dev/null | grep -q ":5000" && docker stop $$id; \
	done 2>/dev/null || true

free-port:
	-lsof -ti:5000 | xargs kill -9 2>/dev/null || true

run: build stop free-port
	sed 's/^export //' .live.env .tools2.env > .docker.env && \
	docker run --env-file .docker.env -p 5000:5000 price-predictor-api && \
	rm -f .docker.env