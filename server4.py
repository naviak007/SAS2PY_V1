import requests
import uuid
import re
import os

API_KEY = "sk-7UFeDwZu8Uqn3BQ2JkYaJm7TAmShweqZQvlYtcQyhlw"
FLOW_URL = "https://ignisflow.infogain.com/api/v1/run/da5175f4-c20e-4dba-a373-5336abf9fead"


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

    match = re.search(r"```python([\s\S]*?)```", text)
    return match.group(1).strip() if match else text


input_folder = r"C:\Users\Naviak\OneDrive - Infogain India Private Limited\Desktop\Sas2PYMain\SAS2PY_V1\sasCodeSourceFolder"
output_folder = r"C:\Users\Naviak\OneDrive - Infogain India Private Limited\Desktop\Sas2PYMain\SAS2PY_V1\pythonCodeDestinationFolder"

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.endswith(".sas"):
        input_file_path = os.path.join(input_folder, filename)

        with open(input_file_path, "r") as f:
            sas_code = f.read()

        try:
            result = call_api(sas_code)
            py_code = extract_pyspark(result)

            base_name = os.path.splitext(filename)[0]
            output_file_name = f"{base_name}_converted.py"
            output_file_path = os.path.join(output_folder, output_file_name)

            with open(output_file_path, "w") as f:
                f.write(py_code)

            print(f"Converted: {filename} → {output_file_name}")

        except Exception as e:
            print(f"Error processing {filename}: {e}")

print("All files processed.")