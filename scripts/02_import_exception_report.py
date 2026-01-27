import os
import glob
import shutil
import pandas as pd
import psycopg2
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv(override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
INPUT_DIR = "input"
ARCHIVE_DIR = "imported"
FILE_PATTERN = os.getenv("PATTERN_EXCEPTION_REPORT", "ExceptionReport*.csv")

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
        'Location Name': 'location_name',
        'Display Name': 'staff',
        'Access Level Name': 'role_access',
        'Timesheet Date': 'ts_date',
        'Timesheet Start Time': 'ts_start',
        'Timesheet End Time': 'ts_end',
        'Timesheet Meal Break (Total)': 'ts_mealbreak',
        'Timesheet Rest Break (Total)': 'ts_restbreak',
        'Timesheet Total Time': 'total_hours',
        'Validation Flag': 'validation_flag',
        'Time Approved': 'time_approved',
        'Is Leave': 'is_leave'
    }
    
    # Check for missing columns
    missing = [c for c in mapping.keys() if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
        return False
        
    # Standardize names and select
    df_clean = df[list(mapping.keys())].rename(columns=mapping).copy()
    
    # 3. Format ts_date for Postgres
    # Assuming ts_date is YYYY-MM-DD or similar based on sample
    df_clean['ts_date'] = pd.to_datetime(df_clean['ts_date']).dt.date

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
        cur.copy_from(buffer, 'exception_report', sep=',', columns=cols)
        
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
        print(f"🔍 No exception report files found in {INPUT_DIR} matching {FILE_PATTERN}")
        return
        
    for f in files:
        process_file(f)

if __name__ == "__main__":
    main()
