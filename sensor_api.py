# sensor_api.py
import requests
import pandas as pd
from datetime import datetime, timedelta

def sensor_list(api_key, secret_key):
    """Fetch the list of sensors from the Monnit API."""
    print("Fetching sensor list...")
    url = "https://www.imonnit.com/json/SensorListFull"
    headers = {"APIKeyID": api_key, "APISecretKey": secret_key}

    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print(f"Retrieved {len(data['Result'])} sensors.")
        df = pd.DataFrame(data['Result'])
        return df['SensorID'].unique()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return []

def sensor_data(sensor_id, from_date, to_date, api_key, secret_key):
    """Fetch sensor data for a given sensor ID between two dates."""
    print(f"Fetching data for sensor {sensor_id} from {from_date} to {to_date}...")
    url = "https://www.imonnit.com/json/SensorDataMessages"
    headers = {"APIKeyID": api_key, "APISecretKey": secret_key}
    params = {"sensorID": sensor_id, "fromDate": from_date, "toDate": to_date}

    response = requests.post(url, headers=headers, data=params)
    if response.status_code == 200:
        data = response.json()
        return data['Result']
    else:
        print(f"Error fetching sensor {sensor_id}: {response.status_code}")
        return []
