
import pandas as pd

def generate_orders_report(file_path, fiscal_year, quarter):
    # Load complaints and order sheets
    complaints_df = pd.read_excel(file_path, sheet_name="RD_All_Complaints")
    order_sheets = {f'F{year}': f'RD_Orders_F{year}' for year in range(2022, 2026)}
    orders_dfs = {year: pd.read_excel(file_path, sheet_name=sheet) for year, sheet in order_sheets.items()}

    # Date ranges per quarter
    quarter_ranges = {
        'Q1': ('03-01', '05-31'),
        'Q2': ('06-01', '08-31'),
        'Q3': ('09-01', '11-30'),
        'Q4': ('12-01', '02-28') if int(fiscal_year[-4:]) % 4 != 0 else ('12-01', '02-29')
    }

    start_date = f"{int(fiscal_year[-4:]) - 1}-{quarter_ranges[quarter][0]}"
    end_date = f"{int(fiscal_year[-4:])}-{quarter_ranges[quarter][1]}"

    if fiscal_year in orders_dfs:
        orders_df = orders_dfs[fiscal_year]
        orders_filtered = orders_df[
            (orders_df['Order Date'] >= start_date) & 
            (orders_df['Order Date'] <= end_date)
        ]
        orders_summary = orders_filtered.groupby('Business Unit')['Order No'].nunique().reset_index(name='Unique Orders')
        return orders_summary
    else:
        raise ValueError(f"Data for fiscal year {fiscal_year} is not available.")
