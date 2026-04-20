from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import uuid
import re

app = Flask(__name__)
CORS(app)

API_KEY = "sk-7UFeDwZu8Uqn3BQ2JkYaJm7TAmShweqZQvlYtcQyhlw"
FLOW_URL = "https://ignisflow.infogain.com/api/v1/run/da5175f4-c20e-4dba-a373-5336abf9fead"



# ---------- GLOBAL PROGRESS ----------
progress = {
    "current": 0,
    "total": 0
}


# ---------- ADD LINE NUMBERS ----------
def add_line_numbers(code):
    lines = code.split("\n")
    return "\n".join(f"{i+1}: {line}" for i, line in enumerate(lines))


# ---------- PREPROCESS ----------
def preprocess_sas(code):

    def remove_comments_line(line):
        result = ""
        i = 0
        in_single = False
        in_double = False

        while i < len(line):

            char = line[i]

            # ---------- HANDLE QUOTES ----------
            if char == "'" and not in_double:
                in_single = not in_single
                result += char
                i += 1
                continue

            if char == '"' and not in_single:
                in_double = not in_double
                result += char
                i += 1
                continue

            # ---------- REMOVE INLINE COMMENTS (* ... ;) ----------
            if not in_single and not in_double:
                if char == '*':

                    # look ahead for semicolon
                    semicolon_index = line.find(';', i)

                    if semicolon_index != -1:
                        # skip comment
                        i = semicolon_index + 1
                        continue

            result += char
            i += 1

        return result

    # ---------- REMOVE BLOCK COMMENTS (/* ... */) SAFELY ----------
    def remove_block_comments(code):
        result = ""
        i = 0
        in_single = False
        in_double = False

        while i < len(code):

            if code[i] == "'" and not in_double:
                in_single = not in_single
                result += code[i]
                i += 1
                continue

            if code[i] == '"' and not in_single:
                in_double = not in_double
                result += code[i]
                i += 1
                continue

            # detect /* ... */
            if not in_single and not in_double and code[i:i+2] == "/*":
                end = code.find("*/", i+2)
                if end != -1:
                    # preserve line breaks
                    comment_block = code[i:end+2]
                    result += "\n" * comment_block.count("\n")
                    i = end + 2
                    continue

            result += code[i]
            i += 1

        return result

    # ---------- STEP 1: REMOVE BLOCK COMMENTS ----------
    code = remove_block_comments(code)

    # ---------- STEP 2: PROCESS LINE BY LINE ----------
    lines = code.split("\n")
    cleaned_lines = []

    for line in lines:
        stripped = line.strip()

        # FULL LINE COMMENT (* ... ;) only if outside quotes
        if stripped.startswith("*") and stripped.endswith(";"):
            cleaned_lines.append("")
            continue

        cleaned_line = remove_comments_line(line)
        cleaned_lines.append(cleaned_line.rstrip())

    return "\n".join(cleaned_lines)
# ---------- SMART SPLITTER ----------
def split_sas_blocks(code):

    lines = code.split("\n")
    blocks = []
    current_block = []

    block_start_keywords = ("data", "proc", "%macro")
    block_end_keywords = ("run;", "quit;", "%mend")

    def clean_line(line):
        # remove line numbers like "12: "
        return re.sub(r'^\s*\d+:\s*', '', line).lower().strip()

    for line in lines:

        cleaned = clean_line(line)

        # ---------- START BLOCK ----------
        if any(cleaned.startswith(k) for k in block_start_keywords):
            if current_block:
                blocks.append("\n".join(current_block).strip())
                current_block = []

        current_block.append(line)

        # ---------- END BLOCK ----------
        if any(cleaned.startswith(k) for k in block_end_keywords):
            blocks.append("\n".join(current_block).strip())
            current_block = []

    # leftover (options, libname etc.)
    if current_block:
        blocks.append("\n".join(current_block).strip())

    return [b for b in blocks if b]

# ---------- SAFE API CALL ----------
def convert_block(block):

    payload = {
        "input_value": f"""
You are an expert SAS to PySpark converter.

Convert this SAS code into PySpark:

{block}
""",
        "input_type": "chat",
        "output_type": "chat",
        "session_id": str(uuid.uuid4())
    }

    print(">>> BEFORE API CALL")

    try:
        r = requests.post(
            FLOW_URL,
            json=payload,
            headers={"x-api-key": API_KEY},
            timeout=20
        )

        print(">>> AFTER API CALL")

    except requests.exceptions.Timeout:
        print("  TIMEOUT")
        return "ERROR: API TIMEOUT"

    except requests.exceptions.RequestException as e:
        print(f"  REQUEST ERROR: {str(e)}")
        return "ERROR: REQUEST FAILED"

    try:
        data = r.json()
    except:
        print("  INVALID JSON RESPONSE")
        return "ERROR: INVALID RESPONSE"

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

    global progress

    sas_code = request.json["sas"]

    sas_code = preprocess_sas(sas_code)
    sas_code = add_line_numbers(sas_code)

    blocks = split_sas_blocks(sas_code)

    progress["current"] = 0
    progress["total"] = len(blocks)

    results = []

    for i, block in enumerate(blocks):

        print(f"\n🚀 Processing block {i+1}/{len(blocks)}")
        print(f"Block size: {len(block)} chars")

        progress["current"] = i + 1

        try:
            py = convert_block(block)

            results.append({
                "sas": block,
                "pyspark": py,
                "status": "success"
            })

        except Exception as e:
            print(f"  ERROR in block {i+1}: {str(e)}")

            results.append({
                "sas": block,
                "pyspark": "",
                "status": "failed",
                "error": str(e)
            })

    return jsonify({
        "blocks": results
    })


# ---------- PROGRESS ----------
@app.route("/progress", methods=["GET"])
def get_progress():
    return jsonify(progress)


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

    except Exception as e:
        return jsonify({
            "pyspark": "",
            "status": "failed",
            "error": str(e)
        })


if __name__ == "__main__":
    app.run(port=5000, debug=True)