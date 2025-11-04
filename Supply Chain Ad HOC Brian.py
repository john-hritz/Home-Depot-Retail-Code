import os
import pandas as pd

# Define the base directory and data tables directory
BASE_DIR = os.path.join(os.getcwd(), 'THD Data Warehouse')
DATA_TABLES_DIR = os.path.join(BASE_DIR, 'Data_Tables')

# Define the files to check
PARQUET_FILES = {
    'merch_hierarchy': 'merch_hierarchy.parquet',
    'store_list': 'store_list.parquet',
    'dsr_list': 'dsr_list.parquet',
    'online_sales': 'online_sales.parquet',
    'online_website_analysis': 'online_website_analysis.parquet',
    'store_pos': 'store_pos.parquet'
}

def count_rows_in_parquet(file_path):
    try:
        # Read the Parquet file and count rows
        df = pd.read_parquet(file_path)
        row_count = len(df)
        return row_count
    except FileNotFoundError:
        return "Error: File not found."
    except Exception as e:
        return f"Error: {str(e)}"

def main():
    # Ensure the data tables directory exists
    if not os.path.exists(DATA_TABLES_DIR):
        print(f"Directory not found: {DATA_TABLES_DIR}")
        return

    # Check each file
    for file_key, file_name in PARQUET_FILES.items():
        file_path = os.path.join(DATA_TABLES_DIR, file_name)
        result = count_rows_in_parquet(file_path)
        print(f"Number of rows in {file_name}: {result}")

if __name__ == "__main__":
    main()