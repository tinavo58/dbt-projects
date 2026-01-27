import pandas as pd
import os
import glob
import shutil
import subprocess
import psycopg2
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
INPUT_DIR = "input"
ARCHIVE_DIR = "imported"
SEED_PATH = os.path.join('seeds', 'payroll_export.csv')
FILE_PATTERN = os.getenv("PATTERN_PAYROLL_EXPORT", "PayrollExport*.csv")

def process_file(file_path):
    print(f"📂 Processing: {os.path.basename(file_path)}")
    
    # 1. Load CSV with encoding fallback
    try:
        # Try UTF-8 first, then common alternatives for exports
        for encoding in ['utf-8', 'latin1', 'cp1252', 'utf-16']:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                print(f"  📖 Read successful using {encoding} encoding.")
                break
            except (UnicodeDecodeError, Exception):
                continue
        else:
            # If all failed, let pandas raise the final error
            df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Error reading CSV '{file_path}': {e}")
        return False
        
    # 2. Map Columns
    mapping = {
        'Employee payroll ID': 'employee_id',
        'Timesheet location name': 'location_name',
        'Timesheet location code': 'location_code',
        'Display name': 'staff',
        'Timesheet date': 'ts_date',
        'Timesheet start time': 'ts_start',
        'Timesheet end time': 'ts_end',
        'Timesheet mealbreak duration': 'ts_mealbreak',
        'Timesheet total hours': 'ts_total_hours',
        'Timesheet cost': 'ts_cost'
    }
    
    # Check for missing columns
    missing = [c for c in mapping.keys() if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
        return False
        
    # Standardize names and select
    df_clean = df[list(mapping.keys())].rename(columns=mapping).copy()
    
    # 3. Format Date (YYYY-MM-DD) and calculate Week Ending (we)
    def standardize_date(date_str):
        if '-' in str(date_str) and len(str(date_str)) == 10:
            return pd.to_datetime(date_str).date()
        try:
            return pd.to_datetime(date_str, dayfirst=True).date()
        except:
            return date_str

    df_clean['ts_date'] = df_clean['ts_date'].apply(standardize_date)
    
    def get_we(dt):
        if pd.isna(dt): return None
        # dt is already a date object from standardize_date
        # 0=Mon, 6=Sun
        days_to_sunday = (6 - dt.weekday()) % 7
        return dt + pd.Timedelta(days=days_to_sunday)

    df_clean['we'] = df_clean['ts_date'].apply(get_we)
    
    # 4. Standardize Employee ID
    def format_id(x):
        if pd.isna(x) or str(x).strip() == '':
            return None
        try:
            return int(float(x))
        except:
            return x

    df_clean['employee_id'] = df_clean['employee_id'].apply(format_id)
    
    # 5. Save to Seed (keep for backward compatibility)
    print(f"  💾 Updating seed file: {SEED_PATH}")
    df_clean.to_csv(SEED_PATH, index=False)
    
    # 6. Inject into Database (deputy_ts)
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # 6a. Idempotency: Create Temp Table
        cur.execute("CREATE TEMP TABLE temp_ts (LIKE deputy_ts INCLUDING ALL) ON COMMIT DROP")
        
        # 6b. Prepare buffer for COPY
        cols = ['employee_id', 'location_name', 'location_code', 'staff', 'ts_date', 'ts_start', 'ts_end', 'ts_mealbreak', 'ts_total_hours', 'ts_cost', 'we']
        buffer = StringIO()
        df_clean[cols].to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        cur.copy_from(buffer, 'temp_ts', sep=',', columns=cols)
        
        # 6c. DELETE existing records that match (employee_id, ts_date)
        # This handles updates and adjustments efficiently
        cur.execute("""
            DELETE FROM deputy_ts 
            WHERE (employee_id, ts_date) IN (
                SELECT employee_id, ts_date FROM temp_ts
            )
        """)
        rows_deleted = cur.rowcount
        if rows_deleted > 0:
            print(f"  🧹 Cleared {rows_deleted} existing records for matching employee/dates.")
            
        # 6d. INSERT from temp
        cur.execute("INSERT INTO deputy_ts SELECT * FROM temp_ts")
        print(f"  ✅ Injected {len(df_clean)} rows into deputy_ts.")
        
        conn.commit()
    except Exception as e:
        print(f"  ❌ Database injection error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

    # 7. Trigger dbt seed
    try:
        print("  🏗️ Running 'dbt seed --full-refresh -s payroll_export'...")
        subprocess.run(["dbt", "seed", "--full-refresh", "-s", "payroll_export"], check=True)
        print("  ✅ dbt seed completed successfully.")
    except Exception as e:
        print(f"  ⚠️ Warning: dbt seed failed: {e}")

    # 8. Archive file
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
        
    dest_path = os.path.join(ARCHIVE_DIR, os.path.basename(file_path))
    if os.path.exists(dest_path):
        name, ext = os.path.splitext(os.path.basename(file_path))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_path = os.path.join(ARCHIVE_DIR, f"{name}_{timestamp}{ext}")
        
    shutil.move(file_path, dest_path)
    print(f"  📦 Archived to: {dest_path}")
    return True

def main():
    if not os.path.exists(INPUT_DIR):
        print(f"⚠️ Input directory '{INPUT_DIR}' does not exist.")
        return

    pattern = os.path.join(INPUT_DIR, FILE_PATTERN)
    files = glob.glob(pattern)
    
    if not files:
        print(f"🔍 No files found in {INPUT_DIR} matching {FILE_PATTERN}")
        # Also check for exactly "payroll_export.csv" in case it's there
        fallback = os.path.join(INPUT_DIR, "payroll_export.csv")
        if os.path.exists(fallback):
            process_file(fallback)
        return
        
    for f in files:
        process_file(f)

if __name__ == "__main__":
    main()
