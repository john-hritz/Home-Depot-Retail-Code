import polars as pl
import os
from datetime import datetime

# Define file path
input_file = "C:\\Users\\john.hritz\\OneDrive - Leviton\\Documents\\Python\\THD Data Warehouse\\Data_Tables\\combined_data.parquet"
log_file = os.path.join(os.path.dirname(input_file), "check_row_count_log.txt")

# Read the Parquet file
try:
    df = pl.read_parquet(input_file)
    row_count = df.height
    print(f"Row count: {row_count}")
    with open(log_file, 'a') as log:
        log.write(f"Checked at {datetime.now().strftime('%H:%M:%S %Z on %m/%d/%Y')}: Row count = {row_count}\n")
except Exception as e:
    print(f"Error: {str(e)}")
    with open(log_file, 'a') as log:
        log.write(f"Error at {datetime.now().strftime('%H:%M:%S %Z on %m/%d/%Y')}: {str(e)}\n")