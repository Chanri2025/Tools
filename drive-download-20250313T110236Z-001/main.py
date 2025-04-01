import requests
import pandas as pd
import json
import time
import sys
import os

# Determine the folder of the EXE (or script)
if getattr(sys, 'frozen', False):
    # Running in a PyInstaller one-file bundle
    script_dir = os.path.dirname(sys.executable)
else:
    # Normal Python execution
    script_dir = os.path.dirname(os.path.abspath(__file__))

csv_path = os.path.join(script_dir, "dma_data.csv")

def get_parameters():
    params_file = os.path.join(script_dir, "params.json")
    print("DEBUG: script_dir =", script_dir)
    print("DEBUG: Checking for params_file =", params_file)
    print("DEBUG: Directory contents:", os.listdir(script_dir))

    if os.path.exists(params_file):
        try:
            with open(params_file, "r") as file:
                params = json.load(file)
            print(f"✅ Loaded parameters from {params_file}: {params}")
            return params
        except Exception as e:
            print(f"❌ Error reading parameters file: {e}")
            sys.exit(1)
    else:
        print("❌ No parameters file found. Please place params.json in the same folder as the EXE.")
        sys.exit(1)

def fetch_dma_data(params):
    url = "https://dmaapi.ictsbm.com/api/DmaDashboard/GetDmaDetails"
    headers = {
        "Content-Type": "application/json",
        "XApiKey": "c3188948-0de5-4649-96a3-e1ad3593e30f",
        "dmaloginid": "dma"
    }
    response = requests.post(url, headers=headers, json=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ API Error: {response.status_code}, {response.text}")
        return None

def process_data(response):
    if response:
        try:
            df = pd.json_normalize(response)
            df.to_csv(csv_path, index=False)
            print(f"✅ Data saved successfully to: {csv_path}")
            time.sleep(3)
        except Exception as e:
            print(f"❌ Error processing data: {e}")
    else:
        print("❌ No data received from API.")

if __name__ == "__main__":
    parameters = get_parameters()
    api_response = fetch_dma_data(parameters)
    process_data(api_response)
