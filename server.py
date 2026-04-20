from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import uuid

app = Flask(__name__)

# ENABLE CORS
CORS(app)

API_KEY = "sk-7UFeDwZu8Uqn3BQ2JkYaJm7TAmShweqZQvlYtcQyhlw"

#FLOW_URL = "https://ignisflow.infogain.com/api/v1/run/8d24b4ec-9b41-42ea-a598-7f6f3864da30" #v1 flow
FLOW_URL = "https://ignisflow.infogain.com/api/v1/run/da5175f4-c20e-4dba-a373-5336abf9fead"  #v2 flow


@app.route("/convert", methods=["POST"])
def convert():

    data = request.json
    sas_code = data["sas"]

    payload = {
        "input_value": sas_code,
        "input_type": "chat",
        "output_type": "chat",
        "session_id": str(uuid.uuid4())
    }

    r = requests.post(
        FLOW_URL,
        json=payload,
        headers={"x-api-key": API_KEY}
    )

    return jsonify(r.json())

if __name__ == "__main__":
    app.run(port=5000)