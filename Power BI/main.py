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

# Paths for CSV outputs
data_csv_path = os.path.join(script_dir, "dma_data.csv")         # For "data" array
chart_csv_path = os.path.join(script_dir, "dma_chart_data.csv")  # For "chartData" array (optional)

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
            # -------------------------------------------------------------
            # 1) Extract the "data" array and save as CSV
            # -------------------------------------------------------------
            data_array = response.get("data", [])
            df_data = pd.DataFrame(data_array)
            df_data.to_csv(data_csv_path, index=False)
            print(f"✅ 'data' array saved successfully to: {data_csv_path}")

            # -------------------------------------------------------------
            # 2) (Optional) Extract the "chartData" array and save as CSV
            #    If you don't need chartData, you can remove this section.
            # -------------------------------------------------------------
            chart_array = response.get("chartData", [])
            if chart_array:
                df_chart = pd.DataFrame(chart_array)
                df_chart.to_csv(chart_csv_path, index=False)
                print(f"✅ 'chartData' array saved successfully to: {chart_csv_path}")

            time.sleep(3)

        except Exception as e:
            print(f"❌ Error processing data: {e}")
    else:
        print("❌ No data received from API.")

if __name__ == "__main__":
    parameters = get_parameters()
    api_response = fetch_dma_data(parameters)
    process_data(api_response)
