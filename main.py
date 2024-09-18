from flask import Flask, request, jsonify
import json
from db_exchange import exchange_assets
from ttl_exposure import view_account
from deribit_bal import show_deribit_bal
from spot_exchange import show_spot_pnl

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
def db_exchange():
    if request.is_json:
        
        # Parse Payload
        payload = request.get_json()
        print(f"Payload: {payload}")
        event = {"function_to_run": payload.get("function_to_run")}

        # Call the db_exchange function
        json_result = exchange_assets(event)
        result = json.loads(json_result.get('body'))

        return jsonify(result), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400
    
@app.route("/ttl_exposure", methods=["POST"])
def ttl_exposure():
    if request.is_json:
        
        # Parse Payload
        payload = request.get_json()
        print(f"Payload: {payload}")

        event = {
            "start_date": payload.get("start_date"),
            "end_date": payload.get("end_date")
            }

        # Call the account_view function
        json_result = view_account(event)
        result = json.loads(json_result.get('body'))

        return jsonify(result), 200
    else:
        return jsonify({"error": "Request must be JSON"}), 400

@app.route("/deribit_bal", methods=["GET"])
def deribit_bal():

    # Call the show_deribit_bal function
    json_result = show_deribit_bal()
    result = json.loads(json_result.get('body'))

    return jsonify(result), 200

@app.route("/spot_pnl", methods=["GET"])
def spot_pnl():

    # Call the show_deribit_bal function
    json_result = show_spot_pnl()
    result = json.loads(json_result.get('body'))

    return jsonify(result), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)