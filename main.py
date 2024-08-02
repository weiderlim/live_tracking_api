from flask import Flask, request, jsonify
import json
from db_exchange import lambda_handler

app = Flask(__name__)

@app.route("/")
def hello():
    data = {
        "exchange": "binance", 
        "PIC": "Jansen", 
        "balance":12345
    }
    
    return jsonify(data), 200

@app.route("/db_exchange", methods=["POST"])
def process():
    if request.is_json:
        
        # Parse Payload
        payload = request.get_json()
        print(f"Payload: {payload}")
        event = {"function_to_run": payload.get("function_to_run")}

        # Call the lambda_handler function
        json_result = lambda_handler(event, "")
        result = json.loads(json_result.get('body'))

        return jsonify(result), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)