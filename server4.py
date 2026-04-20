import requests
import uuid
import re
import os

FLOW_URL = "https://ignisflow.infogain.com/api/v1/run/da5175f4-c20e-4dba-a373-5336abf9fead"
API_KEY =  "sk-7UFeDwZu8Uqn3BQ2JkYaJm7TAmShweqZQvlYtcQyhlw"

def call_api(sas_code):
    payload = {
        "input_value": sas_code,
        "input_type": "chat",
        "output_type": "chat",
        "session_id": str(uuid.uuid4())
    }

    response = requests.post(
        FLOW_URL,
        json=payload,
        headers={"x-api-key": API_KEY},
        timeout=120
    )

    response.raise_for_status()
    return response.json()


def extract_pyspark(result):
    text = (
        result.get("outputs", [{}])[0]
              .get("outputs", [{}])[0]
              .get("results", {})
              .get("message", {})
              .get("text", "")
    )

    import re
    match = re.search(r"```python([\s\S]*?)```", text)

    return match.group(1).strip() if match else text

input_path = "SAS2PY_V1\sasCodeSourceFolder"

output_path = "SAS2PY_V1\pythonCodeDestinationFolder"

with open(input_path, "r") as f:
    sas_code = f.read()

result = call_api(sas_code)
py_code = extract_pyspark(result)

with open(output_path, "w") as f:
    f.write(py_code)

print("Done")