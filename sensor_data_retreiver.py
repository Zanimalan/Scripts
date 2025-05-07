# sensor_data_retriever.py
import os
import pandas as pd
from datetime import datetime, timedelta
from sensor_api import sensor_data

def get_monthly_data(sensor_ids, year, month, api_key, secret_key, output_folder):
    """Download and save monthly data per sensor into CSVs."""
    all_data = pd.DataFrame()

    start_date = datetime(year, month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date = next_month - timedelta(days=next_month.day)
    delta = timedelta(days=7)

    print(f"Retrieving data for {len(sensor_ids)} sensors for {year}-{month:02d}...")

    for idx, sensor_id in enumerate(sensor_ids):
        print(f"\nProcessing sensor {sensor_id} ({idx + 1}/{len(sensor_ids)})")
        output_file = os.path.join(output_folder, f"sensor_{sensor_id}_{year}_{month:02d}.csv")

        if os.path.exists(output_file):
            print(f"Data already exists for {sensor_id}, loading from file.")
            sensor_df = pd.read_csv(output_file)
        else:
            sensor_data_list = []
            current_start = start_date
            while current_start <= end_date:
                current_end = min(current_start + delta, end_date)
                from_date = current_start.strftime("%m/%d/%Y")
                to_date = current_end.strftime("%m/%d/%Y")
                data_chunk = sensor_data(sensor_id, from_date, to_date, api_key, secret_key)
                sensor_data_list.extend(data_chunk or [])
                current_start = current_end + timedelta(days=1)

            if sensor_data_list:
                sensor_df = pd.DataFrame(sensor_data_list)
                sensor_df.to_csv(output_file, index=False)
                print(f"Saved sensor data to {output_file}")
            else:
                print(f"No data retrieved for sensor {sensor_id}")
                continue

        if not sensor_df.empty:
            sensor_df['SensorID'] = sensor_id
            all_data = pd.concat([all_data, sensor_df], ignore_index=True)

    print(f"\nTotal records retrieved: {len(all_data)}")
    return all_data
