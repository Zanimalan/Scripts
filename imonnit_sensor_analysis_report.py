import os
import pandas as pd
from fpdf import FPDF

# Define the folder path where processed CSVs and output PDFs will be saved
folder_path = r"C:\Path\To\Your\Output\Folder"

# Function to calculate dynamic column widths for the PDF table
def calculate_col_widths(dataframe, pdf, padding=2, last_column_extra=0):
    """
    Dynamically calculates column widths based on content length so that
    the table fits nicely in an A4 landscape PDF.

    Args:
        dataframe (pd.DataFrame): Data to be displayed in the table.
        pdf (FPDF): PDF object.
        padding (int): Extra space added to column width.
        last_column_extra (int): Extra width reserved for the last column (used for comments).

    Returns:
        list: Calculated widths for each column.
    """
    total_width = 277  # A4 landscape width (297mm) minus 20mm for margins
    usable_width = total_width - last_column_extra
    col_widths = []

    for col in dataframe.columns[:-1]:  # Exclude the last column temporarily
        max_width = pdf.get_string_width(str(col)) + padding  # Start with header width
        for value in dataframe[col]:
            cell_width = pdf.get_string_width(str(value)) + padding
            max_width = max(max_width, cell_width)
        col_widths.append(max_width)

    # Scale column widths if they exceed the usable page width
    total_used_width = sum(col_widths)
    scaling_factor = usable_width / total_used_width if total_used_width > usable_width else 1
    col_widths = [width * scaling_factor for width in col_widths]
    col_widths.append(last_column_extra)  # Add extra width for the last column
    return col_widths

# Function to draw a table in the PDF
def draw_table(pdf, dataframe, col_widths):
    """
    Draws the headers and data rows of the table in the PDF.
    Automatically handles page breaks and repeats headers on new pages.

    Args:
        pdf (FPDF): PDF object.
        dataframe (pd.DataFrame): Data to draw.
        col_widths (list): Column widths.
    """
    # Draw table headers
    pdf.set_font("Arial", style="B", size=8)
    for i, col in enumerate(dataframe.columns):
        pdf.cell(col_widths[i], 10, col, border=1, align="C")
    pdf.ln()

    # Draw table rows
    pdf.set_font("Arial", size=8)
    for _, row in dataframe.iterrows():
        if pdf.get_y() > 180:  # Add new page if content overflows
            pdf.add_page()
            pdf.set_font("Arial", style="B", size=8)
            for i, col in enumerate(dataframe.columns):
                pdf.cell(col_widths[i], 10, col, border=1, align="C")
            pdf.ln()
            pdf.set_font("Arial", size=8)
        for i, col in enumerate(dataframe.columns):
            pdf.cell(col_widths[i], 10, str(row[col]), border=1)
        pdf.ln()

# Function to convert the processed CSV file into a formatted PDF report
def csv_to_pdf(year, month):
    """
    Loads processed sensor data from a CSV file and generates a
    professionally formatted PDF report with tables.

    Args:
        year (int): Year of the report.
        month (int): Month of the report.
    """
    filename = f"processed_analysis_{year}_{month:02d}.csv"
    file_path = os.path.join(folder_path, filename)

    # Verify if CSV file exists
    if not os.path.exists(file_path):
        print(f"File '{filename}' not found in folder '{folder_path}'.")
        return

    # Load the CSV data into a DataFrame
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    # Remove 'SensorID' column and sort by 'SensorName'
    if 'SensorID' in df.columns:
        df = df.drop(columns=['SensorID'])
    if 'SensorName' in df.columns:
        df = df.sort_values(by='SensorName')

    # Format numeric columns to 1 decimal place
    for col in df.select_dtypes(include='number').columns:
        df[col] = df[col].apply(lambda x: f"{x:.1f}")

    # Define columns to be included in the full report
    relevant_columns = [
        'SensorName', 'UOM', 'min', 'max', 'mean', 'avg_out_of_spec', 
        'non_compliant_hours', 'non_compliant_days', 'Compliant Yes/No'
    ]
    df = df[relevant_columns]

    # Filter non-compliant sensors for a secondary table
    non_compliant_df = df[df['Compliant Yes/No'] == 'No'].copy()
    if not non_compliant_df.empty:
        non_compliant_df = non_compliant_df[[
            'SensorName', 'UOM', 'avg_out_of_spec', 
            'non_compliant_hours', 'non_compliant_days'
        ]]
        # Add blank column for Head of Department comments
        non_compliant_df['HOD comments'] = ''

    # Initialize PDF
    pdf = FPDF(orientation="L", unit="mm", format="A4")  # Landscape mode
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", style="B", size=10)

    # Title
    month_name = pd.to_datetime(f"{year}-{month:02d}-01").strftime("%B %Y")
    pdf.cell(0, 10, f"Sensor Report: {month_name}", ln=True, align="C")
    pdf.ln(10)

    # Draw Full Report Table
    pdf.set_font("Arial", size=8)
    col_widths = calculate_col_widths(df, pdf, last_column_extra=0)
    draw_table(pdf, df, col_widths)

    # Draw Non-Compliant Table
    if not non_compliant_df.empty:
        pdf.add_page()
        pdf.set_font("Arial", style="B", size=10)
        pdf.cell(0, 10, "Non-Compliant Sensors Report", ln=True, align="C")
        pdf.ln(10)

        col_widths_non_compliant = calculate_col_widths(non_compliant_df, pdf, last_column_extra=80)
        draw_table(pdf, non_compliant_df, col_widths_non_compliant)

    # Save PDF
    output_file = os.path.join(folder_path, f"processed_analysis_{year}_{month:02d}.pdf")
    try:
        pdf.output(output_file)
        print(f"PDF saved successfully: {output_file}")
    except Exception as e:
        print(f"Error saving PDF: {e}")

# Prompt user for year and month input
year = input("Enter the year (e.g., 2024): ")
month = input("Enter the month (e.g., 9 for September): ")

# Validate user input and run the report generation
try:
    year = int(year)
    month = int(month)
    csv_to_pdf(year, month)
except ValueError:
    print("Invalid input. Please enter numeric values for year and month.")
