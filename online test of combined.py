import polars as pl
sales_df = pl.read_parquet('online_sales.parquet')
unique_combinations = sales_df.group_by(['week', 'oms id +', 'fulfillment channel +', 'fulfillment channel name +']).agg(pl.len()).select(pl.len())
print(unique_combinations.collect())