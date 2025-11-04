import os
import re
import xlrd
from pathlib import Path
from datetime import datetime
import pandas as pd
from openpyxl.styles import Font, Alignment

# Define base directory
user_profile = os.environ.get('USERPROFILE', os.getcwd())
base_directory = os.path.join(user_profile, 'OneDrive - Leviton', 'Documents', 'Python', 'Pricing Files')
if not os.path.isdir(base_directory):
    base_directory = os.getcwd()

# Columns to include (excluding Trade1, Trade2, Trade3, Comm Code)
include_cols = [
    'UPC','UPC +','UPC Inner Pack I25','UPC Standard Pack I25','Item','Life Cycle','Description',
    'Customer Item','PPC / ICC','UOM','Net Price','List Price','End Date','Std Pk (SPW)','Inner Pk (IPW)',
    'Ctns per Tier','Tiers per Pallet','Length','Height','Depth','Cube','Weight','Pack Validation','Nafta','COO'
]

# Clean text function
_clean_edges = re.compile(r'^[\s=\"\']*|[\s\"\'"]*$')
def clean_text(val):
    if pd.isna(val): return val
    s = str(val).strip()
    if s.startswith('=\"') and s.endswith('\"'):
        s = s[2:-1]
    return _clean_edges.sub('', s)

# Remove HTML tags
def strip_html_tags(text):
    return re.sub(r'<[^>]+>', '', text)

# Extract description value from file (clean HTML tags)
def get_a4_cell_value(xls_path: str) -> str:
    try:
        # Read raw file content and extract description
        with open(xls_path, 'rb') as f:
            content = f.read().decode(errors='ignore')
        match = re.search(r'Description\s*:\s*(.+)', content)
        if match:
            raw_value = match.group(1).strip()
            return strip_html_tags(raw_value)
    except:
        pass
    # Fallback to xlrd for older files
    try:
        book = xlrd.open_workbook(xls_path)
        sheet = book.sheet_by_index(0)
        for row_idx in range(min(15, sheet.nrows)):
            cell_val = str(sheet.cell_value(row_idx, 0)).strip()
            if cell_val.lower().lstrip().startswith("description :"):
                if row_idx + 1 < sheet.nrows:
                    return str(sheet.cell_value(row_idx + 1, 0)).strip()
                else:
                    return Path(xls_path).stem
        return Path(xls_path).stem
    except:
        return Path(xls_path).stem

# Read pricing table
def read_pricelist_table(xls_path: str) -> pd.DataFrame:
    dfs = pd.read_html(xls_path, header=0)
    for df in dfs:
        cols = [str(c).strip() for c in df.columns]
        if 'UPC' in cols and 'Item' in cols:
            df.columns = cols
            return df
    raise RuntimeError(f"Did not find the expected pricing table in {xls_path}")

# Build full UPC
def build_full_upc(upc, upc_plus):
    u = clean_text(upc)
    p = clean_text(upc_plus)
    if not u or str(u).lower() == 'nan':
        return pd.NA
    try:
        u = str(int(float(re.sub(r'\D', '', str(u)))))
    except:
        u = ''
    try:
        p = '' if not p or str(p).lower() == 'nan' else str(int(float(re.sub(r'\D', '', str(p)))))
    except:
        p = ''
    return f"0{u}{p}"

# Find all .xls files
xls_files = sorted(str(p) for p in Path(base_directory).glob('*.xls'))
if not xls_files:
    raise SystemExit(f"No .xls files found in: {base_directory}")

frames = []
for path in xls_files:
    df = read_pricelist_table(path)

    # Ensure all included columns exist
    for c in include_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Clean included columns
    for c in include_cols:
        df[c] = df[c].map(clean_text)

    # Column A: Price File from cleaned description, Column B: Full UPC
    df.insert(0, 'Price File', get_a4_cell_value(path))
    df.insert(1, 'Full UPC', df.apply(lambda r: build_full_upc(r.get('UPC'), r.get('UPC +')), axis=1))

    # Drop rows missing both UPC and Item
    df = df[~(df['UPC'].isna() & df['Item'].isna())]

    frames.append(df)

merged = pd.concat(frames, ignore_index=True)

# Preserve leading zeros
for col in ['Price File','Full UPC','UPC','UPC +','UPC Inner Pack I25','UPC Standard Pack I25','Item','Customer Item','PPC / ICC','UOM']:
    if col in merged.columns:
        merged[col] = merged[col].astype('string')

# Write to Excel with Calibri size 7
from datetime import datetime
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
out_path = Path(base_directory) / "Retail_Pricing_File.xlsx"
with pd.ExcelWriter(out_path, engine='openpyxl') as xw:
    merged.to_excel(xw, index=False, sheet_name='Merged')
    workbook = xw.book
    worksheet = xw.sheets['Merged']
    font = Font(name='Calibri', size=7)
    for row in worksheet.iter_rows():
        for cell in row:
            cell.font = font
    workbook = xw.book
    worksheet = xw.sheets['Merged']

    # Set font for all cells
    font = Font(name='Calibri', size=7)
    for row in worksheet.iter_rows():
        for cell in row:
            cell.font = font

    # Freeze panes at row 1
    worksheet.freeze_panes = worksheet['A2']

    # Turn on filter for row 1
    worksheet.auto_filter.ref = worksheet.dimensions

    # Wrap text for header row
    for cell in worksheet[1]:
        cell.alignment = Alignment(wrap_text=True)

    # Set zoom to 80%
    worksheet.sheet_view.zoomScale = 90
    

# Move processed .xls files to Archive folder
# Define archive folder
archive_folder = Path(base_directory) / 'Archive'
archive_folder.mkdir(exist_ok=True)

# Get today's date in YYYY-MM-DD format
today_str = datetime.today().strftime('%Y-%m-%d')

# Move files and rename with date
for file_path in xls_files:
    try:
        original_name = Path(file_path).stem
        extension = Path(file_path).suffix
        new_name = f"{original_name}_{today_str}{extension}"
        dest_path = archive_folder / new_name
        os.rename(file_path, dest_path)
        print(f"Moved {file_path} -> {dest_path}")
    except Exception as e:
        print(f"Failed to move {file_path}: {e}")

print(f"Done. Wrote {len(merged):,} rows to {out_path}")