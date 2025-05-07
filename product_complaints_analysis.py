
import pandas as pd

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

def count_product_complaints(complaints_df, fiscal_year, quarter):
    filtered = filter_data_by_fy_and_quarter(complaints_df, 'Case Created Date', fiscal_year, quarter)
    product_complaints = filtered[filtered['Problem Sub-Type'] == 'Product']
    return product_complaints.shape[0], product_complaints['Simple Product Code'].nunique()

def analyze_complaints(complaints_df, fiscal_year, quarter):
    filtered = filter_data_by_fy_and_quarter(complaints_df, 'Case Created Date', fiscal_year, quarter)
    return filtered.groupby('Cost Centre').size().reset_index(name='Number of Complaints')

def clean_and_count_unique_items(df, column_name):
    df[column_name] = df[column_name].str.strip().str.lower()
    return df[column_name].nunique()

def analyze_orders(orders_df, fiscal_year, quarter):
    filtered = filter_data_by_fy_and_quarter(orders_df, 'Order Date', fiscal_year, quarter)
    return pd.DataFrame({
        'Unique Orders': [filtered['Order No'].nunique()],
        'Unique Products Ordered': [clean_and_count_unique_items(filtered, 'Item')]
    })

def generate_detailed_report(file_path, fiscal_year, quarter):
    complaints_df = load_data(file_path, 'RD_All_Complaints')
    orders_df = load_data(file_path, f'RD_Orders_{fiscal_year}')
    product_complaint_count, unique_product_types = count_product_complaints(complaints_df, fiscal_year, quarter)
    return {
        'Complaint Summary': analyze_complaints(complaints_df, fiscal_year, quarter),
        'Product Complaints': {
            'Number of Product Complaints': product_complaint_count,
            'Unique Product Types Complained About': unique_product_types
        },
        'Order Summary': analyze_orders(orders_df, fiscal_year, quarter)
    }
