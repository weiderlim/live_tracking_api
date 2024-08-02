from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route("/")
def hello():
    data = {
        "exchange": "binance", 
        "PIC": "Jansen", 
        "balance":12345
    }
    
    return jsonify(data), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)