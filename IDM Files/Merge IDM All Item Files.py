import os
import pandas as pd
from pathlib import Path
import warnings

# Suppress the openpyxl style warning (optional, but cleans up output)
warnings.filterwarnings("ignore", message="Workbook contains no default style")

# Function to locate OneDrive Documents folder or fallback to Documents
def get_base_directory():
    home = Path.home()
    # Check for OneDrive directory (common patterns)
    onedrive_docs = [
        home / "OneDrive" / "Documents",
        home / "OneDrive - Leviton" / "Documents",  # Adjust for your specific OneDrive name
    ]
    for path in onedrive_docs:
        if path.exists():
            return path
    # Fallback to standard Documents folder
    return home / "Documents"

# Define the base folder and subfolder dynamically
base_folder = get_base_directory() / "Python"
idm_folder = base_folder / "IDM Files"

# Ensure the IDM Files subfolder exists
if not idm_folder.exists():
    idm_folder.mkdir(parents=True)  # Create parent directories if needed
    print(f"Created directory: {idm_folder}")
else:
    print(f"Directory exists: {idm_folder}")

# Initialize an empty list to store DataFrames
all_dataframes = []

# Define the output filename to exclude it from processing
output_filename = "merged_output.xlsx"

# Iterate through all Excel files in the IDM Files subfolder, excluding the output file
for file in idm_folder.glob("*.xlsx"):
    if file.name == output_filename:
        print(f"Skipping output file: {file}")
        continue  # Skip the merged_output.xlsx file to avoid self-referencing
    print(f"Processing file: {file}")
    # Read the Excel file, assuming data is in 'Sheet1'
    # Optionally specify engine='openpyxl' here if issues persist with other files
    df = pd.read_excel(file, sheet_name="Sheet1")
    all_dataframes.append(df)

# Check if any files were found
if not all_dataframes:
    print("No Excel files found in the 'IDM Files' subfolder.")
else:
    # Concatenate all DataFrames
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Define the output file path
    output_file = idm_folder / "merged_output.xlsx"
    
    # Save the merged DataFrame to a new Excel file
    merged_df.to_excel(output_file, index=False, sheet_name="Sheet1")
    print(f"Merged file saved to: {output_file}")