# run_full_analysis.py
import os
from sensor_api import sensor_list
from sensor_data_retriever import get_monthly_data
from sensor_data_processor import process_sensor_data

def full_analysis(api_key, secret_key, year, month, limits_filepath, output_folder):
    """Run the complete pipeline: retrieve data, process, and save report."""
    print(f"\nStarting full analysis for {year}-{month:02d}")

    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    sensor_ids = sensor_list(api_key, secret_key)
    if not sensor_ids.any():
        print("No sensors found. Exiting.")
        return

    monthly_data = get_monthly_data(sensor_ids, year, month, api_key, secret_key, output_folder)
    if monthly_data.empty:
        print("No data to process.")
        return

    processed = process_sensor_data(monthly_data, limits_filepath, output_folder, year, month)
    print("\nAnalysis complete.")
    print(processed)

# Example usage (replace with your own)
if __name__ == "__main__":
    api_key = "YOUR_API_KEY"
    secret_key = "YOUR_SECRET_KEY"
    year = 2025
    month = 1
    limits_filepath = r"path\to\your\limits.csv"
    output_folder = r"path\to\your\output"

    full_analysis(api_key, secret_key, year, month, limits_filepath, output_folder)
