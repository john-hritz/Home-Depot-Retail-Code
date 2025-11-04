import pandas as pd

# Load the cleaned file with UPC2 as text
df = pd.read_excel("book2.xlsx", dtype={'UPC2': str})

# Strip whitespace from column names
df.columns = df.columns.str.strip()

# Group by UPC2 and aggregate
aggregated = df.groupby('UPC2', as_index=False).agg({
    'ITEM STATUS': lambda x: ', '.join(x.dropna().astype(str)),  # keep duplicates
    'ITEM FOR PRINT': lambda x: ', '.join(pd.Series(x).dropna().astype(str).unique())  # unique only
})

# Save the result
aggregated.to_excel("aggregated_output.xlsx", index=False)
