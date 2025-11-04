import pandas as pd
import time
from tqdm import tqdm

# Record start time
start_time = time.time()
print("Starting script execution...")

# Read the Excel file
print("Reading input file 'UPCs.xlsx'...")
df = pd.read_excel('UPCs.xlsx')
print(f"File read successfully. Found {len(df)} rows.")

# Clean UPC column: remove leading zeros and spaces
print("Cleaning UPC column...")
df['UPC_cleaned'] = df['UPC'].astype(str).str.replace(r'^\s*0+', '', regex=True).str.replace(r'\s+', '', regex=True)
print("UPC cleaning complete.")

# Define obsolete statuses
obsolete_statuses = ['Z-Obsolete', 'Obsolete', 'Sell Off', 'Final Bld', 'Sell Out', 'Build Out']

# Create a flag for obsolete statuses
print("Flagging obsolete statuses...")
df['is_obsolete'] = df['ITEM_STATUS'].isin(obsolete_statuses)
print("Obsolete status flagging complete.")

# Group by cleaned UPC to check for duplicates
print("Identifying duplicate UPCs...")
upc_counts = df.groupby('UPC_cleaned').size()
duplicate_upcs = upc_counts[upc_counts > 1].index
single_upcs = upc_counts[upc_counts == 1].index
print(f"Found {len(duplicate_upcs)} UPCs with duplicates and {len(single_upcs)} single-occurrence UPCs.")

# Initialize list to store rows to keep
rows_to_keep = []

# Process duplicate UPCs with progress bar
print("Processing duplicate UPCs...")
for upc in tqdm(duplicate_upcs, desc="Duplicate UPCs"):
    upc_rows = df[df['UPC_cleaned'] == upc]
    
    # If any non-obsolete status exists, keep only non-obsolete rows
    if not upc_rows['is_obsolete'].all():
        rows_to_keep.append(upc_rows[~upc_rows['is_obsolete']])
    else:
        # If all are obsolete, keep all rows
        rows_to_keep.append(upc_rows)

# Process single-occurrence UPCs
print("Processing single-occurrence UPCs...")
for upc in single_upcs:
    rows_to_keep.append(df[df['UPC_cleaned'] == upc])
print("All UPCs processed.")

# Combine all rows to keep
print("Combining processed rows...")
result_df = pd.concat(rows_to_keep)
print(f"Combined {len(result_df)} rows.")

# Aggregate ITEM_STATUS and ITEM_NUMBER by UPC_cleaned
print("Aggregating ITEM_STATUS and ITEM_NUMBER...")
agg_df = result_df.groupby('UPC_cleaned').agg({
    'ITEM_STATUS': lambda x: ','.join(x.astype(str)),
    'ITEM_NUMBER': lambda x: ','.join(x.astype(str))
}).reset_index()
print(f"Aggregation complete. Result has {len(agg_df)} rows.")

# Rename columns for clarity
agg_df.columns = ['UPC', 'ITEM_STATUS', 'ITEM_NUMBER']

# Write to a new Excel file with a new tab
print("Writing output to 'UPCs_processed.xlsx'...")
with pd.ExcelWriter('UPCs_processed.xlsx', engine='openpyxl') as writer:
    agg_df.to_excel(writer, sheet_name='Processed_UPCs', index=False)
print("Output file written successfully.")

# Print execution time
end_time = time.time()
print(f"Script completed in {end_time - start_time:.2f} seconds.")