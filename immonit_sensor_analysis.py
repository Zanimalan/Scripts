import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os

# Retrieve the list of available sensors from the iMonnit API
def sensor_list(api_key, secret_key):
    print("Fetching sensor list...")
    url = "https://www.imonnit.com/json/SensorListFull"
    headers = {
        "APIKeyID": api_key,
        "APISecretKey": secret_key
    }
    
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print(f"Successfully retrieved sensor list with {len(data['Result'])} sensors.")
    else:
        print(f"Error fetching sensor list: {response.status_code}")
        print(response.text)
        return []
    
    df = pd.DataFrame(data['Result'])
    unique_sensor_ids = df['SensorID'].unique()
    
    return unique_sensor_ids

# Fetch historical data for a given sensor within a date range
def sensor_data(sensor_id, from_date, to_date, api_key, secret_key):
    print(f"Fetching data for sensor {sensor_id} from {from_date} to {to_date}...")
    url = "https://www.imonnit.com/json/SensorDataMessages"
    headers = {
        "APIKeyID": api_key,
        "APISecretKey": secret_key
    }
    
    params = {
        "sensorID": sensor_id,
        "fromDate": from_date,
        "toDate": to_date
    }
    
    response = requests.post(url, headers=headers, data=params)
    
    if response.status_code == 200:
        print(f"Successfully retrieved data for sensor {sensor_id}.")
        data = response.json()
        return data['Result']
    else:
        print(f"Error fetching data for sensor {sensor_id}: {response.status_code}")
        print(response.text)
        return []

# Parse iMonnit's custom date format (/Date(...)/) into Python datetime
def parse_custom_date(date_str):
    """Convert `/Date(...)` format to datetime."""
    if date_str.startswith('/Date('):
        timestamp = int(date_str[6:-2]) / 1000  # Convert milliseconds to seconds
        return datetime.utcfromtimestamp(timestamp)
    return pd.NaT  # Return NaT for unrecognized formats

# Retrieve and store monthly sensor data for multiple sensors
def get_monthly_data(sensor_ids, year, month, api_key, secret_key, output_folder):
    all_data = pd.DataFrame()
    start_date = datetime(year, month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date = next_month - timedelta(days=next_month.day)
    delta = timedelta(days=7)  # Fetch data in 7-day chunks

    print(f"Retrieving data for {len(sensor_ids)} sensors for the month {year}-{month:02d}...")
    
    for sensor_index, sensor_id in enumerate(sensor_ids):
        print(f"\nProcessing sensor {sensor_id} ({sensor_index + 1}/{len(sensor_ids)})...")
        output_file = os.path.join(output_folder, f"sensor_{sensor_id}_{year}_{month:02d}.csv")
        
        # Load existing data if previously saved
        if os.path.exists(output_file):
            print(f"Data for sensor {sensor_id} for {year}-{month:02d} already exists. Loading from file.")
            sensor_df = pd.read_csv(output_file)
        else:
            sensor_data_list = []
            current_start = start_date
            while current_start <= end_date:
                current_end = min(current_start + delta, end_date)
                from_date = current_start.strftime("%m/%d/%Y")
                to_date = current_end.strftime("%m/%d/%Y")
                
                data_chunk = sensor_data(sensor_id, from_date, to_date, api_key, secret_key)
                if data_chunk:
                    sensor_data_list.extend(data_chunk)
                
                current_start = current_end + timedelta(days=1)
            
            if sensor_data_list:
                sensor_df = pd.DataFrame(sensor_data_list)
                sensor_df.to_csv(output_file, index=False)
                print(f"Sensor data for {sensor_id} saved to {output_file}")
            else:
                print(f"No data retrieved for sensor {sensor_id}.")
                continue
        
        # Append to the combined dataset
        if not sensor_df.empty:
            sensor_df['SensorID'] = sensor_id  # Ensure SensorID is present
            all_data = pd.concat([all_data, sensor_df], ignore_index=True)
        
        print(f"Completed data retrieval for sensor {sensor_id}.")
    
    print(f"\nAll data for the month retrieved. Total records: {len(all_data)}.")
    return all_data

# Process the monthly data by applying limits and summarizing compliance
def process_sensor_data(monthly_data, limits_filepath, output_folder, year, month):
    print("\nStarting data processing...")
    
    print("Loading limits file...")
    limits = pd.read_csv(limits_filepath, delimiter=';')
    limits_dict = limits.rename(columns={"Min": "lim_min", "Max": "lim_max", "Avg": "lim_avg", "UOM": "uom"})
    limits_dict = limits_dict[["SensorID", "lim_min", "lim_max", "lim_avg", "uom", "SensorName"]]
    limits_dict.set_index("SensorID", inplace=True)
    
    print("Mapping limits, UOM, and SensorName to sensor data...")
    if 'SensorID' not in monthly_data.columns:
        raise KeyError("'SensorID' column is missing in monthly data. Please check the input data structure.")
    if 'PlotValue' not in monthly_data.columns:
        raise KeyError("'PlotValue' column is missing in monthly data. Please check the input data structure.")
    
    # Ensure PlotValue is numeric
    monthly_data['PlotValue'] = pd.to_numeric(monthly_data['PlotValue'], errors='coerce')
    monthly_data = monthly_data.dropna(subset=['PlotValue'])

    # Convert message date to datetime
    monthly_data['timestamp'] = monthly_data['MessageDate'].apply(parse_custom_date)

    # Map limits to each sensor reading
    monthly_data['lim_min'] = monthly_data['SensorID'].map(lambda x: limits_dict.at[x, 'lim_min'] if x in limits_dict.index else None)
    monthly_data['lim_max'] = monthly_data['SensorID'].map(lambda x: limits_dict.at[x, 'lim_max'] if x in limits_dict.index else None)
    monthly_data['lim_avg'] = monthly_data['SensorID'].map(lambda x: limits_dict.at[x, 'lim_avg'] if x in limits_dict.index else None)
    monthly_data['uom'] = monthly_data['SensorID'].map(lambda x: limits_dict.at[x, 'uom'] if x in limits_dict.index else None)
    monthly_data['SensorName'] = monthly_data['SensorID'].map(lambda x: limits_dict.at[x, 'SensorName'] if x in limits_dict.index else None)

    # Check for compliance with specified limits
    monthly_data['non_compliant'] = (
        (monthly_data['PlotValue'] < monthly_data['lim_min']) |
        (monthly_data['PlotValue'] > monthly_data['lim_max'])
    )

    print("Aggregating sensor data...")
    # Aggregate and summarize results
    agg_data = monthly_data.groupby('SensorID').agg(
        SensorName=('SensorName', 'first'),
        UOM=('uom', 'first'),
        min=('PlotValue', 'min'),
        max=('PlotValue', 'max'),
        mean=('PlotValue', 'mean'),
        lim_min=('lim_min', 'first'),
        lim_max=('lim_max', 'first'),
        lim_avg=('lim_avg', 'first'),
        avg_out_of_spec=('PlotValue', lambda x: x[monthly_data.loc[x.index, 'non_compliant']].mean()),
        non_compliant_hours=('non_compliant', 'sum'),
        non_compliant_days=('timestamp', lambda x: x[monthly_data.loc[x.index, 'non_compliant']].dt.date.nunique())
    ).reset_index()

    print("Performing non-compliance checks...")
    # Mark sensors as compliant or not
    agg_data['Compliant Yes/No'] = agg_data.apply(
        lambda row: 'No' if row['non_compliant_hours'] > 0 else 'Yes', axis=1
    )

    # Save processed data
    processed_file = os.path.join(output_folder, f"processed_analysis_{year}_{month:02d}.csv")
    agg_data.to_csv(processed_file, index=False)
    print(f"Processed analysis saved to {processed_file}")
    
    print("\nData processing complete.")
    return agg_data

# Full pipeline: from fetching to processing sensor data
def full_analysis(api_key, secret_key, year, month, limits_filepath, output_folder):
    print(f"Starting full analysis for {year}-{month:02d}...")
    
    sensor_ids = sensor_list(api_key, secret_key)
    if len(sensor_ids) == 0:
        print("No sensors found. Exiting analysis.")
        return
    
    monthly_data = get_monthly_data(sensor_ids, year, month, api_key, secret_key, output_folder)
    if monthly_data.empty:
        print("No data available for processing.")
        return
    
    print("\nProcessing the retrieved monthly data...")
    processed_analysis = process_sensor_data(monthly_data, limits_filepath, output_folder, year, month)
    print("\nFinal results:")
    print(processed_analysis)


# ---------------------
# Example usage section
# ---------------------

# Replace with your actual API credentials
api_key = "YOUR_API_KEY_HERE"
secret_key = "YOUR_SECRET_KEY_HERE"

# Analysis parameters
year = 2024
month = 12

# Provide generic example file paths
limits_filepath = r'./data/limits.csv'
output_folder = r'./output'

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Execute full analysis pipeline
full_analysis(api_key, secret_key, year, month, limits_filepath, output_folder)
