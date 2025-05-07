# sensor_data_processor.py
import pandas as pd
import os
from datetime import datetime

def parse_custom_date(date_str):
    """Convert Monnit `/Date(...)` to Python datetime."""
    if date_str.startswith('/Date('):
        timestamp = int(date_str[6:-2]) / 1000
        return datetime.utcfromtimestamp(timestamp)
    return pd.NaT

def process_sensor_data(monthly_data, limits_filepath, output_folder, year, month):
    """Process sensor data against limits and save the analysis."""
    print("Loading limits...")
    limits = pd.read_csv(limits_filepath, delimiter=';')
    limits_dict = limits.rename(columns={"Min": "lim_min", "Max": "lim_max", "Avg": "lim_avg",
                                         "UOM": "uom", "SensorName": "SensorName"})
    limits_dict = limits_dict.set_index("SensorID")

    # Ensure numeric
    monthly_data['PlotValue'] = pd.to_numeric(monthly_data['PlotValue'], errors='coerce')
    monthly_data = monthly_data.dropna(subset=['PlotValue'])

    # Convert dates
    monthly_data['timestamp'] = monthly_data['MessageDate'].apply(parse_custom_date)

    # Map limits
    for col in ['lim_min', 'lim_max', 'lim_avg', 'uom', 'SensorName']:
        monthly_data[col] = monthly_data['SensorID'].map(
            lambda x: limits_dict.at[x, col] if x in limits_dict.index else None
        )

    # Non-compliant check
    monthly_data['non_compliant'] = (
        (monthly_data['PlotValue'] < monthly_data['lim_min']) |
        (monthly_data['PlotValue'] > monthly_data['lim_max'])
    )

    # Aggregate
    agg = monthly_data.groupby('SensorID').agg(
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

    agg['Compliant Yes/No'] = agg['non_compliant_hours'].apply(lambda x: 'No' if x > 0 else 'Yes')

    # Save
    processed_file = os.path.join(output_folder, f"processed_analysis_{year}_{month:02d}.csv")
    agg.to_csv(processed_file, index=False)
    print(f"Saved processed analysis to {processed_file}")

    return agg
