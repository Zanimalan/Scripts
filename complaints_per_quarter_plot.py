
import pandas as pd
import matplotlib.pyplot as plt

def load_data(file_path, sheet_name):
    return pd.read_excel(file_path, sheet_name=sheet_name)

def filter_data_by_fy_and_quarter(df, date_column, fiscal_year, quarter):
    quarter_ranges = {
        'Q1': ('03-01', '05-31'),
        'Q2': ('06-01', '08-31'),
        'Q3': ('09-01', '11-30'),
        'Q4': ('12-01', '02-28')
    }
    start_month, end_month = quarter_ranges[quarter]
    fy_map = {'F2022': ('2021', '2022'), 'F2023': ('2022', '2023'), 'F2024': ('2023', '2024'), 'F2025': ('2024', '2025')}
    fiscal_year_start, fiscal_year_end = fy_map[fiscal_year]
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    start_date = f"{fiscal_year_start}-{start_month}"
    end_date = f"{fiscal_year_end}-{end_month}" if quarter == 'Q4' else f"{fiscal_year_start}-{end_month}"
    return df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]

def count_complaints_by_quarter(complaints_df, fiscal_year, max_quarter):
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    result = {}
    for quarter in quarters:
        if quarters.index(quarter) > quarters.index(max_quarter):
            result[quarter] = None
        else:
            filtered = filter_data_by_fy_and_quarter(complaints_df, 'Case Created Date', fiscal_year, quarter)
            result[quarter] = filtered.shape[0]
    return result

def plot_complaints_per_quarter(complaints_per_quarter, fiscal_year):
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    complaints = [complaints_per_quarter.get(q, 0) if complaints_per_quarter[q] is not None else 0 for q in quarters]
    plt.figure(figsize=(8, 6))
    bars = plt.bar([f"{q} {fiscal_year}" for q in quarters], complaints, color='cornflowerblue')
    for i, bar in enumerate(bars):
        if complaints_per_quarter[quarters[i]] is not None:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, int(yval), ha='center', va='bottom')
    plt.title(f'Number of Complaints per Quarter (up to {max([q for q in quarters if complaints_per_quarter[q] is not None])})')
    plt.xlabel('Quarter')
    plt.ylabel('Number of Complaints')
    plt.show()
