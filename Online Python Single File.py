import polars as pl
import os
import argparse
from datetime import datetime

def combine_parquet_files(
    input_dir: str,
    calendar_file: str = 'calendar.parquet',
    sales_file: str = 'online_sales.parquet',
    classification_file: str = 'online_classification.parquet',
    website_file: str = 'online_website_anaylsis.parquet',
    scorecard_file: str = 'BA_scorecard.parquet',
    stores_file: str = 'online_stores.parquet',
    output_file: str = 'combined_data.parquet'
) -> dict:
    """
    Combine multiple Parquet files into a single Parquet file.
    Creates a row for every week and oms id + combination from sales and website data,
    distributing website metrics evenly across sales rows or carrying over with nulls if no sales.
    
    Args:
        input_dir: Directory containing the input Parquet files.
        calendar_file, sales_file, etc.: Names of the input Parquet files.
        output_file: Name of the output Parquet file.
    
    Returns:
        Dict with 'output_file' (path to result) and 'error' (None or error message).
    """
    try:
        print(f"Starting process at {datetime.now().strftime('%H:%M:%S %Z on %m/%d/%Y')}...")
        log_file = os.path.join(input_dir, 'combine_log.txt')
        with open(log_file, 'a') as log:
            log.write(f"Started at {datetime.now().strftime('%H:%M:%S %Z on %m/%d/%Y')}\n")

        # Define file paths
        file_paths = {
            'calendar': os.path.join(input_dir, calendar_file),
            'sales': os.path.join(input_dir, sales_file),
            'classification': os.path.join(input_dir, classification_file),
            'website': os.path.join(input_dir, website_file),
            'scorecard': os.path.join(input_dir, scorecard_file),
            'stores': os.path.join(input_dir, stores_file)
        }

        # Verify files exist
        for key, path in file_paths.items():
            if not os.path.exists(path):
                error_msg = f"Error: File {path} does not exist at {datetime.now().strftime('%H:%M:%S %Z')}"
                print(error_msg)
                with open(log_file, 'a') as log:
                    log.write(error_msg + '\n')
                return {"output_file": None, "error": f"File {path} does not exist"}
            print(f"Found file: {path} at {datetime.now().strftime('%H:%M:%S %Z')}")
            with open(log_file, 'a') as log:
                log.write(f"Found file: {path} at {datetime.now().strftime('%H:%M:%S %Z')}\n")

        output_path = os.path.join(input_dir, output_file)
        print(f"Output will be saved to: {output_path} at {datetime.now().strftime('%H:%M:%S %Z')}")
        with open(log_file, 'a') as log:
            log.write(f"Output will be saved to: {output_path} at {datetime.now().strftime('%H:%M:%S %Z')}\n")

        if os.path.exists(output_path):
            print(f"Deleting existing output file: {output_path} at {datetime.now().strftime('%H:%M:%S %Z')}")
            with open(log_file, 'a') as log:
                log.write(f"Deleting existing output file: {output_path} at {datetime.now().strftime('%H:%M:%S %Z')}\n")
            os.remove(output_path)

        # Step 1: Collect unique weeks from calendar
        print(f"Loading calendar.parquet at {datetime.now().strftime('%H:%M:%S %Z')}...")
        calendar_df = pl.read_parquet(file_paths['calendar'])
        weeks = calendar_df['week'].unique().to_list()
        print(f"Found {len(weeks)} unique weeks at {datetime.now().strftime('%H:%M:%S %Z')}")

        # Step 2: Load sales and website data for oms id + values
        print(f"Loading oms id + from online_sales.parquet at {datetime.now().strftime('%H:%M:%S %Z')}...")
        sales_df = pl.read_parquet(file_paths['sales'])
        oms_ids_sales = sales_df['oms id +'].unique().to_list()

        print(f"Loading oms id + from online_website_anaylsis.parquet at {datetime.now().strftime('%H:%M:%S %Z')}...")
        website_df = pl.read_parquet(file_paths['website'])
        oms_ids_website = website_df['oms id +'].unique().to_list()

        # Create base DataFrame from union of sales and website data
        print(f"Creating base DataFrame with week and oms id + combinations from sales and website at {datetime.now().strftime('%H:%M:%S %Z')}...")
        base_sales = sales_df.lazy().select(['week', 'oms id +']).with_columns(pl.col('week').cast(pl.Utf8)).unique()
        base_website = website_df.lazy().select(['week', 'oms id +']).with_columns(pl.col('week').cast(pl.Utf8)).unique()
        base_df = pl.concat([base_sales, base_website], how='vertical').unique().with_columns(pl.col('week').cast(pl.Utf8))
        print(f"Base DataFrame created with {base_df.collect().height} combinations at {datetime.now().strftime('%H:%M:%S %Z')}")

        # Step 3: Prepare sales data without aggregation on fulfillment channels
        print(f"Processing sales data at {datetime.now().strftime('%H:%M:%S %Z')}...")
        sales_df_lazy = (
            pl.scan_parquet(file_paths['sales'])
            .with_columns([
                pl.col('week').cast(pl.Utf8),  # Cast week to string
                pl.col('icr store +').cast(pl.Utf8)  # Cast icr store + to string
            ])
            .filter(pl.col('oms id +').is_in(oms_ids_sales))  # Subset based on sales oms id +
        )

        # Step 4: Prepare website data for distribution
        print(f"Processing website data at {datetime.now().strftime('%H:%M:%S %Z')}...")
        website_df_lazy = (
            pl.scan_parquet(file_paths['website'])
            .with_columns([
                pl.col('week').cast(pl.Utf8),  # Cast week to string
                pl.col('icr store +').cast(pl.Utf8) if 'icr store +' in pl.scan_parquet(file_paths['website']).collect_schema() else pl.lit(None)  # Cast icr store + if present
            ])
            .filter(pl.col('oms id +').is_in(oms_ids_website))  # Subset based on website oms id +
        )

        # Step 5: Join and distribute website metrics
        print(f"Joining data at {datetime.now().strftime('%H:%M:%S %Z')}...")
        # First join sales data to base
        result_df_lazy = base_df.join(sales_df_lazy, on=['week', 'oms id +'], how='left')

        # Count sales rows per week and oms id + for distribution
        sales_count = (
            result_df_lazy.group_by(['week', 'oms id +'])
            .agg(pl.len().alias('sales_row_count'))
            .select(['week', 'oms id +', 'sales_row_count'])
        )

        # Join website data and distribute metrics
        result_df_lazy = result_df_lazy.join(
            website_df_lazy, on=['week', 'oms id +'], how='left'
        ).join(
            sales_count, on=['week', 'oms id +'], how='left'
        ).with_columns([
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_product_interaction_conversion') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_product_interaction_conversion')).alias('total_product_interaction_conversion'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_product_interaction_conversion_ly') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_product_interaction_conversion_ly')).alias('total_product_interaction_conversion_ly'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_pip_conversion_rate') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_pip_conversion_rate')).alias('total_pip_conversion_rate'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_pip_conversion_rate_ly') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_pip_conversion_rate_ly')).alias('total_pip_conversion_rate_ly'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_display_avg_rating') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_display_avg_rating')).alias('total_display_avg_rating'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_display_1_star_reviews') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_display_1_star_reviews')).alias('total_display_1_star_reviews'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_display_2_star_reviews') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_display_2_star_reviews')).alias('total_display_2_star_reviews'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_display_3_star_reviews') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_display_3_star_reviews')).alias('total_display_3_star_reviews'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_display_4_star_reviews') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_display_4_star_reviews')).alias('total_display_4_star_reviews'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_display_5_star_reviews') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_display_5_star_reviews')).alias('total_display_5_star_reviews'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_non_buyable_views') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_non_buyable_views')).alias('total_non_buyable_views'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_non_buyable_views_ly') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_non_buyable_views_ly')).alias('total_non_buyable_views_ly'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_current_cost') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_current_cost')).alias('total_current_cost'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_pip_visits') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_pip_visits')).alias('total_pip_visits'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_pip_visits_ly') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_pip_visits_ly')).alias('total_pip_visits_ly'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_product_interaction_visits') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_product_interaction_visits')).alias('total_product_interaction_visits'),
            pl.when(pl.col('sales_row_count').is_not_null() & pl.col('sales_row_count') > 0)
            .then(pl.col('total_product_interaction_visits_ly') / pl.col('sales_row_count'))
            .otherwise(pl.col('total_product_interaction_visits_ly')).alias('total_product_interaction_visits_ly')
        ]).drop('sales_row_count')  # Drop the count column after distribution

        result_df_lazy = result_df_lazy.join(
            pl.scan_parquet(file_paths['classification']), on='oms id +', how='left'
        )
        result_df_lazy = result_df_lazy.join(
            pl.scan_parquet(file_paths['scorecard']).rename({'OMSID': 'oms id +'}),
            on='oms id +', how='left'
        )
        result_df_lazy = result_df_lazy.join(
            pl.scan_parquet(file_paths['stores']).with_columns(pl.col('icr store +').cast(pl.Utf8)),
            on='icr store +', how='left'
        )

        # Step 6: Write to Parquet
        print(f"Writing to Parquet at {datetime.now().strftime('%H:%M:%S %Z')}...")
        result_df_lazy.sink_parquet(output_path)
        print(f"Success: Combined data written to {output_path} at {datetime.now().strftime('%H:%M:%S %Z')}")
        with open(log_file, 'a') as log:
            log.write(f"Success: Combined data written to {output_path} at {datetime.now().strftime('%H:%M:%S %Z')}\n")

        return {"output_file": output_path, "error": None}

    except Exception as e:
        error_msg = f"Failed to combine files at {datetime.now().strftime('%H:%M:%S %Z')}: {str(e)}"
        print(error_msg)
        with open(log_file, 'a') as log:
            log.write(error_msg + '\n')
        return {"output_file": None, "error": error_msg}

def main():
    parser = argparse.ArgumentParser(description="Combine Parquet files with week and oms id + cross join.")
    parser.add_argument(
        "--input_dir",
        default=os.path.join(os.getcwd(), 'THD Data Warehouse', 'Data_Tables'),
        help="Directory containing the input Parquet files (default: ./THD Data Warehouse/Data_Tables)"
    )
    parser.add_argument("--calendar_file", default="calendar.parquet", help="Calendar Parquet file")
    parser.add_argument("--sales_file", default="online_sales.parquet", help="Sales Parquet file")
    parser.add_argument("--classification_file", default="online_classification.parquet", help="Classification Parquet file")
    parser.add_argument("--website_file", default="online_website_anaylsis.parquet", help="Website analysis Parquet file")
    parser.add_argument("--scorecard_file", default="BA_scorecard.parquet", help="Scorecard Parquet file")
    parser.add_argument("--stores_file", default="online_stores.parquet", help="Stores Parquet file")
    parser.add_argument("--output_file", default="combined_data.parquet", help="Output file name (default: combined_data.parquet)")
    args = parser.parse_args()

    result = combine_parquet_files(
        args.input_dir,
        args.calendar_file,
        args.sales_file,
        args.classification_file,
        args.website_file,
        args.scorecard_file,
        args.stores_file,
        args.output_file
    )

    if result["error"]:
        print(f"Error: {result['error']}")
    else:
        print(f"Success: Combined data written to {result['output_file']}")

if __name__ == "__main__":
    main()