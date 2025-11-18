from flask import Flask, jsonify
from src.predictor import Predictor
from loguru import logger

app = Flask(__name__)

predictor = Predictor.from_model_registry(model_name='btc_usd_price_predictor_lasso')
logger.info(f"Predictor initialized")

@app.route('/health', methods=['GET'])
def health():
    return f"I'm healthy"

@app.route('/predict', methods=['POST'])
def predict():
    output = predictor.predict()
    return jsonify(output.to_dict())

if __name__ == '__main__':
    app.run(port=5000, debug=True)