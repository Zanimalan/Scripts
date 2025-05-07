import pandas as pd
from datetime import datetime
from tabulate import tabulate

# Define fiscal year ranges and quarters
fiscal_years = {
    "F2022": {"start": "2021-03-01", "end": "2022-02-28"},
    "F2023": {"start": "2022-03-01", "end": "2023-02-28"},
    "F2024": {"start": "2023-03-01", "end": "2024-02-29"},
    "F2025": {"start": "2024-03-01", "end": "2025-02-28"},
}

quarters = {
    "Q1": {"start": "03-01", "end": "05-31"},
    "Q2": {"start": "06-01", "end": "08-31"},
    "Q3": {"start": "09-01", "end": "11-30"},
    "Q4": {"start": "12-01", "end": "02-28"},
}

# File path to the Excel file
file_path = r"C:\Path\To\Your\Complaints and Orders.xlsx"
complaints_sheet = "RD_All_Complaints"

# Function to calculate start and end dates for fiscal year and quarter
def get_fiscal_quarter_dates(fiscal_year, quarter):
    """
    Returns start and end date for a given fiscal year and quarter.
    """
    fy_start_year = fiscal_years[fiscal_year]["start"][:4]
    qtr_start = f"{fy_start_year}-{quarters[quarter]['start']}"
    qtr_end = f"{fy_start_year}-{quarters[quarter]['end']}" if quarter != "Q4" else fiscal_years[fiscal_year]["end"]

    return pd.to_datetime(qtr_start), pd.to_datetime(qtr_end)

# Function to filter data by fiscal quarter
def filter_data_by_quarter(data, date_column, fiscal_year, quarter):
    """
    Filters the data between the selected quarter dates.
    """
    start_date, end_date = get_fiscal_quarter_dates(fiscal_year, quarter)
    data[date_column] = pd.to_datetime(data[date_column], errors='coerce')
    return data[(data[date_column] >= start_date) & (data[date_column] <= end_date)]

# Function to analyze total orders and complaints with percentage calculation
def analyze_overall_complaints(fiscal_year, quarter):
    """
    Analyzes total complaints vs total unique orders for the given quarter.
    """
    try:
        excel_file = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return

    # Load sheets
    complaints_df = pd.read_excel(excel_file, sheet_name=complaints_sheet)
    orders_sheet = f"RD_Orders_{fiscal_year}"
    orders_df = pd.read_excel(excel_file, sheet_name=orders_sheet)

    # Filter both datasets
    filtered_complaints = filter_data_by_quarter(complaints_df, "Case Created Date", fiscal_year, quarter)
    filtered_orders = filter_data_by_quarter(orders_df, "Order Date", fiscal_year, quarter)

    total_orders = filtered_orders["Order No"].nunique()
    total_complaints = len(filtered_complaints)

    percentage_complaints = (total_complaints / total_orders * 100) if total_orders > 0 else 0

    # Create result DataFrame
    result = pd.DataFrame({
        "Quarter": [quarter],
        "Total Orders": [total_orders],
        "Total Complaints": [total_complaints],
        "Percentage Complaints vs Orders": [round(percentage_complaints, 2)]
    })

    return result

# Function to analyze complaints grouped by business unit
def analyze_complaints_by_unit(fiscal_year, quarter):
    """
    Groups complaints by business unit (Cost Centre) for the given quarter.
    """
    complaints_df = pd.read_excel(file_path, sheet_name=complaints_sheet)
    filtered_complaints = filter_data_by_quarter(complaints_df, "Case Created Date", fiscal_year, quarter)

    # Group by Cost Centre
    complaints_by_unit = filtered_complaints.groupby("Cost Centre").size().reset_index(name="Number of Complaints")
    return complaints_by_unit

# Function to prompt user
def prompt_user_for_fy_and_quarter():
    """
    Prompts user for fiscal year and quarter selection.
    """
    print("Available Fiscal Years:", list(fiscal_years.keys()))
    fiscal_year = input("Enter fiscal year (e.g., F2023): ")

    print("Available Quarters: Q1, Q2, Q3, Q4")
    quarter = input("Enter quarter (e.g., Q1): ")

    return fiscal_year, quarter

# Main program execution
if __name__ == "__main__":
    fiscal_year, quarter = prompt_user_for_fy_and_quarter()

    # Overall analysis
    overall_result = analyze_overall_complaints(fiscal_year, quarter)
    if overall_result is not None:
        print("\n=== Overall Complaints vs Orders ===")
        print(overall_result.to_string(index=False))

    # Complaints by unit
    complaints_by_unit = analyze_complaints_by_unit(fiscal_year, quarter)
    print(f"\n=== Complaints by Business Unit for {fiscal_year} {quarter} ===")
    print(tabulate(complaints_by_unit, headers="keys", tablefmt="fancy_grid"))
