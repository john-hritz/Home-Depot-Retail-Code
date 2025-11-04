import os
import duckdb
import polars as pl
import re

# Define paths
BASE_DIR = os.path.join(os.getcwd(), 'THD Data Warehouse')
DATA_TABLES_DIR = os.path.join(BASE_DIR, 'Data_Tables')
parquet_file_path = os.path.join(DATA_TABLES_DIR, 'online_sales.parquet')

# Check if Parquet file exists
if not os.path.exists(parquet_file_path):
    raise FileNotFoundError(f"Parquet file not found at: {parquet_file_path}")

# Verify the required columns exist using Polars
print("Checking Parquet file schema...")
df_schema = pl.read_parquet(parquet_file_path, n_rows=1)
required_columns = ["online sales $ ly +", "online sales $ +", "online order units +", "week"]

# Check for missing columns
missing_columns = [col for col in required_columns if col not in df_schema.columns]
if missing_columns:
    raise ValueError(f"Missing columns in Parquet file: {missing_columns}")

# Check if data is non-empty
if len(df_schema) == 0:
    raise ValueError("Parquet file is empty.")

# SQL query: group by week, sum sales and units
query = f"""
SELECT 
    week,
    SUM("online order units +") AS order_unit,
    SUM("online sales $ ly +") AS Sales_LY,
    SUM("online sales $ +") AS Sales_TY
FROM read_parquet('{parquet_file_path}')
GROUP BY week
ORDER BY week
"""

# Execute the query
try:
    print("Executing query...")
    result_df = duckdb.sql(query).to_df()
    print("Query executed successfully.")
except Exception as e:
    print(f"Error executing query: {e}")
    exit(1)

# Extract numeric week number from 'week' column (e.g., "Fiscal Week 1 of 2025" -> 1)
def extract_week_number(week_str):
    match = re.search(r'Week (\d{1,2}) of', week_str)
    return int(match.group(1)) if match else 0

result_df['week_number'] = result_df['week'].apply(extract_week_number)
result_df = result_df.sort_values(by='week_number')

# Display all weekly sales and units, formatted with commas and 2 decimals
print("\nWeekly Sales and Units Totals:")
for _, row in result_df.iterrows():
    week = row["week"]
    order_unit = row["order_unit"]
    sales_ly = row["Sales_LY"]
    sales_ty = row["Sales_TY"]
    formatted_sales_ly = f"{sales_ly:,.2f}"
    formatted_sales_ty = f"{sales_ty:,.2f}"
    formatted_units = f"{order_unit:,.0f}"
    print(f"Week {week}: Sales LY = ${formatted_sales_ly}, Sales TY = ${formatted_sales_ty}, Order Units = {formatted_units}")