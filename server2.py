from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import uuid
import re

app = Flask(__name__)
CORS(app)

API_KEY = "sk-7UFeDwZu8Uqn3BQ2JkYaJm7TAmShweqZQvlYtcQyhlw"
FLOW_URL = "https://ignisflow.infogain.com/api/v1/run/da5175f4-c20e-4dba-a373-5336abf9fead"


# ---------- PREPROCESS ----------
def preprocess_sas(code):
    return "\n".join(
        [line.rstrip() for line in code.split("\n") if line.strip() != ""]
    )


# ---------- SMART BLOCK SPLITTER ----------
def split_sas_blocks(code):
    pattern = r'(%macro[\s\S]*?%mend;|data[\s\S]*?run;|proc[\s\S]*?run;)'
    blocks = re.findall(pattern, code, flags=re.IGNORECASE)
    return blocks


# ---------- CALL IGNIS ----------
def convert_block(block):
    payload = {
        "input_value": f"""
You are an expert SAS to PySpark converter.

Convert ONLY this SAS block into PySpark.
Return ONLY clean PySpark code.

SAS BLOCK:
{block}
""",
        "input_type": "chat",
        "output_type": "chat",
        "session_id": str(uuid.uuid4())
    }

    r = requests.post(
        FLOW_URL,
        json=payload,
        headers={"x-api-key": API_KEY}
    )

    data = r.json()

    return (
        data.get("outputs", [{}])[0]
        .get("outputs", [{}])[0]
        .get("results", {})
        .get("message", {})
        .get("text", "")
    )


# ---------- MAIN CONVERT ----------
@app.route("/convert", methods=["POST"])
def convert():

    sas_code = request.json["sas"]

    sas_code = preprocess_sas(sas_code)

    blocks = split_sas_blocks(sas_code)

    results = []

    for block in blocks:
        try:
            py = convert_block(block)
            results.append({
                "sas": block,
                "pyspark": py,
                "status": "success"
            })
        except Exception as e:
            results.append({
                "sas": block,
                "pyspark": "",
                "status": "failed"
            })

    return jsonify({
        "blocks": results
    })


# ---------- RETRY ----------
@app.route("/retry", methods=["POST"])
def retry():

    block = request.json["block"]

    try:
        py = convert_block(block)

        return jsonify({
            "pyspark": py,
            "status": "success"
        })

    except:
        return jsonify({
            "pyspark": "",
            "status": "failed"
        })


if __name__ == "__main__":
    app.run(port=5000)