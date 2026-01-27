import os
import glob
import shutil
import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv(override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
INPUT_DIR = "input"
ARCHIVE_DIR = "imported"
FILE_PATTERN = os.getenv("PATTERN_DAILY_EXCEPTION", "DailyException_*.csv")

def get_week_ending(date_str):
    """Calculates the Sunday following the given date (or the date itself if it's Sunday)."""
    dt = pd.to_datetime(date_str, dayfirst=True)
    # 0 = Monday, 6 = Sunday
    days_to_sunday = (6 - dt.weekday()) % 7
    return (dt + timedelta(days=days_to_sunday)).date()

def process_file(file_path):
    print(f"📂 Processing: {os.path.basename(file_path)}")
    
    # 1. Load CSV
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return False
        
    # 2. Map Columns
    mapping = {
        'Timesheet location name': 'location_name',
        'Display name': 'staff',
        'Timesheet date': 'ts_date',
        'Timesheet start time': 'ts_start',
        'Timesheet end time': 'ts_end',
        'Timesheet total hours': 'total_hours',
        'Timesheet total mealbreak duration': 'ts_mealbreak',
        'Pay rule export code': 'pay_rule',
        'Pay rule hours/units': 'pay_rule_units',
        'Pay rule cost': 'pay_rule_cost'
    }
    
    # Check for missing columns
    missing = [c for c in mapping.keys() if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
        return False
        
    # Standardize names and select
    df_clean = df[list(mapping.keys())].rename(columns=mapping).copy()
    
    # 3. Format ts_date for Postgres
    # Assuming ts_date is DD/MM/YYYY based on sample
    df_clean['ts_date'] = pd.to_datetime(df_clean['ts_date'], dayfirst=True).dt.date

    # 4. Inject into database
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Prepare buffer for COPY
        buffer = StringIO()
        df_clean.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        cols = list(df_clean.columns)
        cur.copy_from(buffer, 'daily_exception_export', sep=',', columns=cols)
        
        conn.commit()
        print(f"✅ Successfully injected {len(df_clean)} rows.")
        
        # 5. Archive file
        if not os.path.exists(ARCHIVE_DIR):
            os.makedirs(ARCHIVE_DIR)
            
        dest_path = os.path.join(ARCHIVE_DIR, os.path.basename(file_path))
        if os.path.exists(dest_path):
            name, ext = os.path.splitext(os.path.basename(file_path))
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_path = os.path.join(ARCHIVE_DIR, f"{name}_{timestamp}{ext}")
            
        shutil.move(file_path, dest_path)
        print(f"📦 Archived to: {dest_path}")
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    if not os.path.exists(INPUT_DIR):
        print(f"⚠️ Input directory '{INPUT_DIR}' does not exist.")
        return

    pattern = os.path.join(INPUT_DIR, FILE_PATTERN)
    files = glob.glob(pattern)
    
    if not files:
        print("🔍 No daily exception files found in input/.")
        return
        
    for f in files:
        process_file(f)

if __name__ == "__main__":
    main()
