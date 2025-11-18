train:
	 source .live.env && poetry run python src/training.py

predict:
	 source .live.env && poetry run python src/predictor.py

api:
	 source .live.env && poetry run python src/api.py

request:
	curl -X POST http://localhost:5000/predict -H "Content-Type: application/json"