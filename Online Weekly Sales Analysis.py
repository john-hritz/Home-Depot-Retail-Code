import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from datetime import datetime
import time
import win32com.client
import pythoncom
import pandas as pd
from pathlib import Path
import logging
import threading
import psutil
import gc
import traceback

# Debug flag: Set to False to send email
test_mode = False

# Set up logging
logging.basicConfig(
    filename=os.path.join('THD Data Warehouse', 'reports', 'analysis.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set paths
DATA_DIR = 'THD Data Warehouse'
output_dir = os.path.join(DATA_DIR, 'reports')
os.makedirs(output_dir, exist_ok=True)

# Function to print and flush
def print_progress(message):
    print(message)
    sys.stdout.flush()
    logging.info(message)

# Function to get memory usage
def get_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = psutil.virtual_memory()
    process_mem = process.memory_info().rss / 1024 / 1024  # MB
    total_used = mem_info.used / 1024 / 1024  # MB
    available = mem_info.available / 1024 / 1024  # MB
    return process_mem, total_used, available

# Function for progress indicator with memory usage
def progress_indicator(stop_event, operation):
    count = 1
    total_ram = psutil.virtual_memory().total / 1024 / 1024  # MB
    while not stop_event.is_set():
        process_mem, total_used, available = get_memory_usage()
        msg = f"{count}. ({operation} - Python Mem: {process_mem:.2f} MB, Total Used: {total_used:.2f} MB, Available: {available:.2f} MB)"
        print_progress(msg)
        if available < 1000:  # Warn if less than 1GB available
            print_progress(f"WARNING: Low available memory ({available:.2f} MB). Risk of swapping.")
            logging.warning(f"Low available memory: {available:.2f} MB during {operation}")
        if total_used / total_ram > 0.75:  # Exit if using >75% of total RAM
            stop_event.set()
            raise MemoryError(f"Memory usage too high: {total_used:.2f} MB used, only {available:.2f} MB available.")
        count += 1
        time.sleep(3)  # 3-second interval for feedback

# Load data lazily with tight filtering
try:
    print_progress("Loading data lazily...")
    start_time = time.time()
    sales = pl.scan_parquet(os.path.join(DATA_DIR, 'Data_Tables', 'online_sales.parquet'))
    # Filter to recent 8 weeks and non-zero sales
    sales = sales.filter(pl.col('online sales $ +') > 0).filter(
        pl.col('week').cast(pl.Utf8).str.contains(r'Fiscal Week (?:[1-8]) of 2025')
    )
    print_progress("Scanned and filtered online_sales.parquet")
    website = pl.scan_parquet(os.path.join(DATA_DIR, 'Data_Tables', 'online_website_anaylsis.parquet'))
    print_progress("Scanned online_website_anaylsis.parquet")
    classification = pl.scan_parquet(os.path.join(DATA_DIR, 'Data_Tables', 'merged_classification.parquet'))
    print_progress("Scanned merged_classification.parquet")
    stores = pl.scan_parquet(os.path.join(DATA_DIR, 'Data_Tables', 'online_stores.parquet'))
    # Pre-filter stores to reduce join size
    stores = stores.filter(pl.col('icr store +').is_not_null())
    print_progress("Scanned and filtered online_stores.parquet")
    logging.info("Successfully scanned all Parquet files")
    process_mem, total_used, available = get_memory_usage()
    print_progress(f"Data scanned in {time.time() - start_time:.2f} seconds. Memory: Python: {process_mem:.2f} MB, Total Used: {total_used:.2f} MB, Available: {available:.2f} MB")
except Exception as e:
    error_msg = f"Failed to scan Parquet files: {str(e)}\n{traceback.format_exc()}"
    print_progress(error_msg)
    logging.error(error_msg)
    raise

# Cast 'icr store +' to String
print_progress("Casting 'icr store +' to Utf8 in sales...")
sales = sales.with_columns(pl.col('icr store +').cast(pl.Utf8))
print_progress("Casting complete in sales.")

print_progress("Casting 'icr store +' to Utf8 in stores...")
stores = stores.with_columns(pl.col('icr store +').cast(pl.Utf8))
print_progress("Casting complete in stores.")

# Join data lazily with streaming
try:
    print_progress("Starting lazy data joins...")
    start_time = time.time()
    
    print_progress("Lazy joining sales to classification on 'oms id +'...")
    stop_event = threading.Event()
    progress_thread = threading.Thread(target=progress_indicator, args=(stop_event, "sales_class join"))
    progress_thread.start()
    sales_class = sales.join(classification, on='oms id +', how='left')
    stop_event.set()
    progress_thread.join()
    process_mem, total_used, available = get_memory_usage()
    print_progress(f"sales_class join (lazy) complete. Memory: Python: {process_mem:.2f} MB, Total Used: {total_used:.2f} MB, Available: {available:.2f} MB")
    
    print_progress("Re-casting 'icr store +' to Utf8 in sales_class...")
    stop_event = threading.Event()
    progress_thread = threading.Thread(target=progress_indicator, args=(stop_event, "re-casting"))
    progress_thread.start()
    sales_class = sales_class.with_columns(pl.col('icr store +').cast(pl.Utf8))
    stop_event.set()
    progress_thread.join()
    print_progress("Re-casting complete in sales_class.")
    
    print_progress("Lazy joining sales_class to stores on 'icr store +'...")
    stop_event = threading.Event()
    progress_thread = threading.Thread(target=progress_indicator, args=(stop_event, "sales_full join"))
    progress_thread.start()
    timeout = 300  # 5 minutes timeout
    join_start = time.time()
    sales_full = sales_class.join(stores, on='icr store +', how='inner').collect(engine="streaming")
    if time.time() - join_start > timeout:
        stop_event.set()
        progress_thread.join()
        raise TimeoutError("Join operation timed out after 5 minutes.")
    stop_event.set()
    progress_thread.join()
    process_mem, total_used, available = get_memory_usage()
    print_progress(f"sales_full join complete. Memory: Python: {process_mem:.2f} MB, Total Used: {total_used:.2f} MB, Available: {available:.2f} MB")
    
    print_progress("Lazy joining website to classification on 'oms id +'...")
    stop_event = threading.Event()
    progress_thread = threading.Thread(target=progress_indicator, args=(stop_event, "website_class join"))
    progress_thread.start()
    website_class = website.join(classification, on='oms id +', how='left').collect(engine="streaming")
    stop_event.set()
    progress_thread.join()
    print_progress("website_class join complete.")
    
    logging.info("Successfully performed data joins")
    print_progress(f"Data joins completed in {time.time() - start_time:.2f} seconds.")
except Exception as e:
    if 'stop_event' in locals():
        stop_event.set()
        progress_thread.join()
    error_msg = f"Join operation failed: {str(e)}\n{traceback.format_exc()}"
    print_progress(error_msg)
    logging.error(error_msg)
    raise

# Clean up memory
del sales, classification, stores
gc.collect()
print_progress("Cleared temporary variables.")

# Extract week number and year
def extract_week(df, df_name):
    try:
        print_progress(f"Extracting week and year for {df_name}...")
        return df.with_columns(
            pl.col('week').cast(pl.Utf8).str.extract(r'Fiscal Week (\d+) of (\d+)').cast(pl.Int32).alias('week_num'),
            pl.col('week').cast(pl.Utf8).str.extract(r'Fiscal Week \d+ of (\d+)').cast(pl.Int32).alias('year')
        )
    except Exception as e:
        error_msg = f"Failed to extract week for {df_name}: {str(e)}\n{traceback.format_exc()}"
        print_progress(error_msg)
        logging.error(error_msg)
        raise

print_progress("Extracting week for sales_full...")
sales_full = extract_week(sales_full, "sales_full")
print_progress("sales_full week extraction complete.")

print_progress("Extracting week for website_class...")
website_class = extract_week(website_class, "website_class")
print_progress("website_class week extraction complete.")

# Find most recent week
try:
    print_progress("Identifying recent week...")
    recent_week = sales_full['week'].max()
    recent_week_num = sales_full.filter(pl.col('week') == recent_week)['week_num'].unique()[0]
    recent_year = sales_full.filter(pl.col('week') == recent_week)['year'].unique()[0]
    logging.info(f"Most recent week: {recent_week}, Week Number: {recent_week_num}, Year: {recent_year}")
    print_progress(f"Recent week identified: {recent_week}")
except Exception as e:
    error_msg = f"Failed to determine recent week: {str(e)}\n{traceback.format_exc()}"
    print_progress(error_msg)
    logging.error(error_msg)
    raise

# Sanitize recent_week for file names
recent_week_safe = recent_week.replace(' ', '_').replace(':', '')
print_progress(f"Safe recent_week for files: {recent_week_safe}")

# Recent week analysis
print_progress("Filtering recent sales...")
recent_sales = sales_full.filter((pl.col('week_num') == recent_week_num) & (pl.col('year') == recent_year))
print_progress("Recent sales filtered.")

# Aggregate sales data by key dimensions
def aggregate_sales(df, group_by, week_filter=True):
    try:
        print_progress(f"Aggregating by {group_by}...")
        start_time = time.time()
        agg = df.group_by(group_by).agg(
            ty_sales=pl.col('online sales $ +').sum(),
            ly_sales=pl.col('online sales $ ly +').sum(),
            ty_units=pl.col('online order units +').sum(),
            ly_units=pl.col('online order units ly +').sum(),
            returns=pl.col('online return $ +').abs().sum(),
            cancels=pl.col('online cancel units +').abs().sum(),
            net_sales=(pl.col('online sales $ +') - pl.col('online return $ +') - 
                       pl.col('online cancel units +') * (pl.col('online sales $ +') / pl.col('online order units +').replace(0, 1))).sum()
        ).with_columns(
            sales_diff=pl.col('ty_sales') - pl.col('ly_sales'),
            sales_growth_pct=((pl.col('ty_sales') / pl.col('ly_sales').replace(0, 1) - 1) * 100),
            return_rate=pl.col('returns') / pl.col('ty_sales').replace(0, 1) * 100,
            cancel_rate=pl.col('cancels') / pl.col('ty_units').replace(0, 1) * 100
        ).sort('sales_diff', descending=True)
        if week_filter:
            agg = agg.filter(pl.col('ty_sales') > 0)
        logging.info(f"Aggregated sales by {group_by}")
        print_progress(f"Aggregated by {group_by} in {time.time() - start_time:.2f} seconds.")
        return agg
    except Exception as e:
        error_msg = f"Failed to aggregate sales by {group_by}: {str(e)}\n{traceback.format_exc()}"
        print_progress(error_msg)
        logging.error(error_msg)
        raise

# Aggregations (reduced to one)
print_progress("Starting aggregations...")
class_agg = aggregate_sales(recent_sales, ['online merch dept +', 'online class +', 'online subclass +'])
print_progress("Aggregations complete.")

# Online only vs shared
print_progress("Calculating classification split...")
classification_split = recent_sales.group_by('online classification +').agg(
    ty_sales=pl.col('online sales $ +').sum(),
    ly_sales=pl.col('online sales $ ly +').sum()
).with_columns(
    sales_share=pl.col('ty_sales') / pl.col('ty_sales').sum() * 100
)
print_progress("Classification split calculated.")

# Ship from type
print_progress("Calculating ship from agg...")
ship_from_agg = recent_sales.group_by('online ship from type +').agg(
    ty_sales=pl.col('online sales $ +').sum(),
    ly_sales=pl.col('online sales $ ly +').sum()
).with_columns(
    sales_share=pl.col('ty_sales') / pl.col('ty_sales').sum() * 100
)
print_progress("Ship from agg calculated.")

# Identify standouts
print_progress("Identifying standouts...")
def get_standouts(agg_df, metric='sales_diff', top_n=5, bottom_n=5):
    top = agg_df.sort(metric, descending=True).head(top_n)
    bottom = agg_df.sort(metric, descending=False).head(bottom_n)
    return top, bottom

class_top, class_bottom = get_standouts(class_agg)
print_progress("Standouts identified.")

# Flag high return/cancel rates
print_progress("Flagging high returns/cancels...")
high_return = class_agg.filter(pl.col('return_rate') > 20)
high_cancel = class_agg.filter(pl.col('cancel_rate') > 20)
print_progress("Flags complete.")

# Trend analysis
def get_trend(df, weeks_back, group_by=None):
    try:
        print_progress(f"Computing {weeks_back}-week trend...")
        start_time = time.time()
        min_week = recent_week_num - weeks_back + 1 if weeks_back > 0 else 1
        trend_df = df.filter((pl.col('week_num') >= min_week) & (pl.col('year') == recent_year))
        if group_by:
            trend_df = trend_df.group_by(['week', *group_by]).agg(
                ty_sales=pl.col('online sales $ +').sum(),
                ly_sales=pl.col('online sales $ ly +').sum()
            )
        else:
            trend_df = df.group_by('week').agg(
                ty_sales=pl.col('online sales $ +').sum(),
                ly_sales=pl.col('online sales $ ly +').sum()
            )
        logging.info(f"Computed trend for {weeks_back} weeks")
        print_progress(f"{weeks_back}-week trend computed in {time.time() - start_time:.2f} seconds.")
        return trend_df.sort('week')
    except Exception as e:
        error_msg = f"Failed to compute trend for {weeks_back} weeks: {str(e)}\n{traceback.format_exc()}"
        print_progress(error_msg)
        logging.error(error_msg)
        raise

eight_week_trend = get_trend(sales_full, 8)
print_progress("8-week trend complete.")
del sales_full
gc.collect()
print_progress("Cleared sales_full.")

# Website conversion rates
conv_ty = None
conv_ly = None
try:
    print_progress("Calculating conversion rates...")
    recent_website = website_class.filter((pl.col('week_num') == recent_week_num) & (pl.col('year') == recent_year))
    ty_visits = recent_website['online pip visits +'].sum()
    ly_visits = recent_website['online pip visits ly +'].sum()
    conv_ty = (recent_website['order count TY'].sum() / (ty_visits if ty_visits != 0 else 1)) * 100
    conv_ly = (recent_website['order count LY'].sum() / (ly_visits if ly_visits != 0 else 1)) * 100
    logging.info(f"Conversion Rates - TY: {conv_ty:.2f}%, LY: {conv_ly:.2f}%")
    print_progress(f"Conversion rates calculated: TY {conv_ty:.2f}%, LY {conv_ly:.2f}%")
except Exception as e:
    error_msg = f"Failed to compute website conversion rates: {str(e)}\n{traceback.format_exc()}"
    print_progress(error_msg)
    logging.error(error_msg)
    conv_ty = 0.0
    conv_ly = 0.0
    print_progress("Continuing with conversion rates set to 0.0%")

del website_class
gc.collect()
print_progress("Cleared website_class.")

# Visualizations
print_progress("Generating plots...")
def plot_trend(trend_df, title, filename, y_label='Sales ($)', hue=None):
    try:
        # Sanitize filename
        safe_filename = filename.replace(' ', '_').replace(':', '')
        df_pd = trend_df.to_pandas()
        plt.figure(figsize=(10, 5))
        if hue:
            sns.lineplot(data=df_pd, x='week', y='ty_sales', hue=hue, marker='o')
            sns.lineplot(data=df_pd, x='week', y='ly_sales', hue=hue, linestyle='--')
        else:
            sns.lineplot(data=df_pd, x='week', y='ty_sales', label='TY', marker='o', color='#1f77b4')
            sns.lineplot(data=df_pd, x='week', y='ly_sales', label='LY', linestyle='--', color='#ff7f0e')
        plt.title(title)
        plt.xlabel('Fiscal Week')
        plt.ylabel(y_label)
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, safe_filename), dpi=100)
        plt.close()
        logging.info(f"Saved plot: {safe_filename}")
        print_progress(f"Saved {safe_filename}")
    except Exception as e:
        error_msg = f"Failed to generate plot {filename}: {str(e)}\n{traceback.format_exc()}"
        print_progress(error_msg)
        logging.error(error_msg)
        raise

# Generate plots (minimal)
plot_trend(eight_week_trend, f'8-Week Sales Trend (Week {recent_week_num})', f'8_week_trend_Fiscal_Week_{recent_week_num}_of_{recent_year}.png')
print_progress("Plots generated.")

# Excel export (minimal)
print_progress("Exporting to Excel...")
try:
    with pl.Config(tbl_rows=20):
        safe_excel_filename = f'class_agg_Fiscal_Week_{recent_week_num}_of_{recent_year}.xlsx'
        class_agg.write_excel(os.path.join(output_dir, safe_excel_filename))
        print_progress(f"Exported {safe_excel_filename}")
    logging.info("Exported key data to Excel")
    print_progress("Excel exports completed.")
except Exception as e:
    error_msg = f"Failed to export to Excel: {str(e)}\n{traceback.format_exc()}"
    print_progress(error_msg)
    logging.error(error_msg)
    raise

# Generate insights report
print_progress("Generating insights...")
sales_growth = 0.0
try:
    ty_sales = recent_sales['online sales $ +'].sum()
    ly_sales = recent_sales['online sales $ ly +'].sum()
    sales_growth = ((ty_sales / (ly_sales if ly_sales != 0 else 1)) - 1) * 100
    logging.info(f"Sales Growth: {sales_growth:.2f}%")
except Exception as e:
    error_msg = f"Failed to compute sales growth: {str(e)}\n{traceback.format_exc()}"
    print_progress(error_msg)
    logging.error(error_msg)
    sales_growth = 0.0
    print_progress("Continuing with sales growth set to 0.0%")

insights = [
    f"Weekly Sales Analysis Report - {recent_week} ({datetime.now().strftime('%Y-%m-%d')})",
    "\nRecent Week Highlights:",
    f"- Total Sales TY: ${recent_sales['online sales $ +'].sum():,.2f}, LY: ${recent_sales['online sales $ ly +'].sum():,.2f}",
    f"- Sales Growth: {sales_growth:.2f}%{' (failed to compute)' if sales_growth == 0.0 else ''}",
    f"- Conversion Rate TY: {conv_ty:.2f}%{' (failed to compute)' if conv_ty == 0.0 else ''}, LY: {conv_ly:.2f}%{' (failed to compute)' if conv_ly == 0.0 else ''}",
    f"- Online Only Sales Share: {classification_split.filter(pl.col('online classification +') == 'Online only')['sales_share'].sum():.2f}%",
    f"- YOW (Warehouse) Sales Share: {ship_from_agg.filter(pl.col('online ship from type +') == 'YOW')['sales_share'].sum():.2f}%",
    "\nActionable Insights:",
    f"- High Return Subclasses: {', '.join(high_return['online subclass +'].to_list()[:3]) or 'None'} (return rate > 20%)",
    f"- High Cancellation Subclasses: {', '.join(high_cancel['online subclass +'].to_list()[:3]) or 'None'} (cancel rate > 20%)"
]
print_progress("Insights generated.")

# Email via Outlook
def send_email():
    try:
        print_progress("Preparing email...")
        pythoncom.CoInitialize()
        outlook = win32com.client.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)
        mail.To = 'john.hritz@leviton.com'
        mail.Subject = f'Weekly Sales Analysis - {recent_week}'
        email_body = f"""
<html>
<body style="font-family: Helvetica, sans-serif;">
<p>Weekly Sales Analysis Report - {recent_week} ({datetime.now().strftime('%Y-%m-%d')})</p>
<ul>
    <li>Total Sales TY: ${recent_sales['online sales $ +'].sum():,.2f}, LY: ${recent_sales['online sales $ ly +'].sum():,.2f}</li>
    <li>Sales Growth: {sales_growth:.2f}%{' (failed to compute)' if sales_growth == 0.0 else ''}</li>
    <li>Conversion Rate TY: {conv_ty:.2f}%{' (failed to compute)' if conv_ty == 0.0 else ''}, LY: {conv_ly:.2f}%{' (failed to compute)' if conv_ly == 0.0 else ''}</li>
    <li>Online Only Sales Share: {classification_split.filter(pl.col('online classification +') == 'Online only')['sales_share'].sum():.2f}%</li>
    <li>YOW (Warehouse) Sales Share: {ship_from_agg.filter(pl.col('online ship from type +') == 'YOW')['sales_share'].sum():.2f}%</li>
</ul>
<p><strong>Actionable Insights:</strong></p>
<ul>
    <li>High Return Subclasses: {', '.join(high_return['online subclass +'].to_list()[:3]) or 'None'} (return rate > 20%)</li>
    <li>High Cancellation Subclasses: {', '.join(high_cancel['online subclass +'].to_list()[:3]) or 'None'} (cancel rate > 20%)</li>
</ul>
<p>See attached files for detailed data and charts.</p>
</body>
</html>
"""
        mail.HTMLBody = email_body
        attachments_count = 0
        for file in os.listdir(output_dir):
            if file.endswith(('.png', '.xlsx')) and f'Fiscal_Week_{recent_week_num}_of_{recent_year}' in file:
                attachment_path = os.path.abspath(os.path.join(output_dir, file))
                if os.path.exists(attachment_path):
                    mail.Attachments.Add(attachment_path)
                    logging.info(f"Attached {attachment_path} to email")
                    print_progress(f"Attached {attachment_path}")
                    attachments_count += 1
                else:
                    print_progress(f"WARNING: File {attachment_path} does not exist and cannot be attached.")
                    logging.warning(f"File {attachment_path} does not exist and cannot be attached.")
        print_progress(f"Total attachments: {attachments_count}")
        if attachments_count == 0:
            print_progress("WARNING: No files found to attach. Sending email without attachments.")
            logging.warning("No files found to attach in output directory.")
        if test_mode:
            print_progress("Test mode: Email not sent. Body:\n" + email_body)
            print_progress("Subject: " + mail.Subject)
        else:
            mail.Send()
            logging.info("Email sent successfully")
            print_progress("Email sent successfully.")
        pythoncom.CoUninitialize()
    except Exception as e:
        error_msg = f"Failed to send email: {str(e)}\n{traceback.format_exc()}"
        print_progress(error_msg)
        logging.error(error_msg)
        raise

# Run analysis
if __name__ == '__main__':
    try:
        print_progress("Starting email send...")
        send_email()
        print_progress(f"Analysis complete for {recent_week}. If no email received, check log and Outlook.")
        logging.info(f"Analysis complete for {recent_week}")
    except Exception as e:
        error_msg = f"Analysis failed: {str(e)}\n{traceback.format_exc()}"
        print_progress(error_msg)
        logging.error(error_msg)
        try:
            pythoncom.CoInitialize()
            outlook = win32com.client.Dispatch('Outlook.Application')
            mail = outlook.CreateItem(0)
            mail.To = 'john.hritz@leviton.com'
            mail.Subject = f'Weekly Sales Analysis - Error for {datetime.now().strftime("%Y-%m-%d")}'
            error_body = f"""
<html>
<body style="font-family: Helvetica, sans-serif;">
<p>Script failed during execution:</p>
<p>Error: {str(e)}</p>
<p>Details:</p>
<pre>{traceback.format_exc()}</pre>
<p>Available files (if any) are in {output_dir}</p>
<p>Check analysis.log for details.</p>
</body>
</html>
"""
            mail.HTMLBody = error_body
            attachments_count = 0
            for file in os.listdir(output_dir):
                if file.endswith(('.png', '.xlsx')) and f'Fiscal_Week_{recent_week_num}_of_{recent_year}' in file:
                    attachment_path = os.path.abspath(os.path.join(output_dir, file))
                    if os.path.exists(attachment_path):
                        mail.Attachments.Add(attachment_path)
                        logging.info(f"Attached {attachment_path} to fallback email")
                        print_progress(f"Attached {attachment_path} to fallback email")
                        attachments_count += 1
                    else:
                        print_progress(f"WARNING: File {attachment_path} does not exist and cannot be attached to fallback email.")
                        logging.warning(f"File {attachment_path} does not exist and cannot be attached to fallback email.")
            if attachments_count == 0:
                print_progress("WARNING: No files attached to fallback email.")
                logging.warning("No files attached to fallback email.")
            mail.Send()
            print_progress("Fallback email sent successfully.")
            logging.info("Fallback email sent successfully")
            pythoncom.CoUninitialize()
        except Exception as email_e:
            error_msg = f"Failed to send fallback email: {str(email_e)}\n{traceback.format_exc()}"
            print_progress(error_msg)
            logging.error(error_msg)
        raise