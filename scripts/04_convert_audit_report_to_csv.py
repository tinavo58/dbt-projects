import pandas as pd
import os
import re
import glob
import shutil
import psycopg2
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv(override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
DOWNLOAD_PATH = os.getenv("DOWNLOAD_PATH")
INPUT_DIR = DOWNLOAD_PATH if DOWNLOAD_PATH else "input"
# Archive should remain in the project folder
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "imported")

# Patterns from env
PATTERN_CASUAL = os.getenv("PATTERN_CASUAL_AUDIT", "PayRunAudit-Casual*.xlsx")
PATTERN_WEEKLY = os.getenv("PATTERN_WEEKLY_AUDIT", "PayRunAudit-Weekly FT*.xlsx")
PATTERN_FN_FT = os.getenv("PATTERN_FN_FT_AUDIT", "PayRunAudit-FN FT*.xlsx")

def extract_date(filename):
    """Extracts YYYYMMDD from filename."""
    match = re.search(r'(\d{8})', filename)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, '%Y%m%d').date()
    return None

def transform_site(location_name):
    """Converts Location Name to 3-letter site code."""
    if pd.isna(location_name):
        return None
    val = str(location_name).strip()
    site_prefix = os.getenv("SITE_PREFIX_REMOVE", "")
    if site_prefix and val.startswith(site_prefix):
        val = val[len(site_prefix):].strip()
    return val[:3].upper()

def process_file(file_path):
    filename = os.path.basename(file_path)
    print(f"\nProcessing: {filename}")
    
    # 1. Determine target table and config based on filename
    is_fn_ft = "FN FT" in filename
    is_weekly_ft = "Weekly FT" in filename
    
    if is_fn_ft:
        target_table = "payruns_fortnightly"
        sheet_name = 'Pay Run Totals'
        header_row = 0
        mapping = {
            'Employee Id': 'employee_id',
            'Employee First Name': 'first_name',
            'Employee Surname': 'surname',
            'Total Hours': 'total_hours',
            'Gross Earnings': 'gross',
            'Post-Tax Deduction': 'post_tax_deduction',
            'Net Earnings': 'net_pay'
        }
        cols = ['weekending', 'employee_id', 'employee', 'total_hours', 'gross', 'post_tax_deduction', 'net_pay']
    elif is_weekly_ft:
        target_table = "payruns_weekly_ft"
        sheet_name = 'Earnings Details'
        header_row = 1
        mapping = {
            'Employee Id': 'employee_id',
            'Employee Name': 'employee',
            'Pay Category Name': 'pay_category',
            'Units': 'units',
            'Location Name': 'site',
            'Rate': 'rate',
            'Gross Earnings': 'gross'
        }
        cols = ['weekending', 'employee_id', 'employee', 'pay_category', 'units', 'site', 'rate', 'gross']
    else:
        target_table = "payruns_cas"
        sheet_name = 'Earnings Details'
        header_row = 1
        mapping = {
            'Employee Id': 'employee_id',
            'Employee Name': 'employee',
            'Pay Category Name': 'pay_category',
            'Units': 'units',
            'Location Name': 'site',
            'Rate': 'rate',
            'Gross Earnings': 'gross'
        }
        cols = ['weekending', 'employee_id', 'employee', 'pay_category', 'units', 'site', 'rate', 'gross']
        
    print(f"  Target Table: {target_table}")

    # 2. Extract weekending from filename
    weekending = extract_date(filename)
    if not weekending:
        print(f"Error: Could not extract date from filename: {filename}")
        return False
        
    # 3. Load Excel
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
    except Exception as e:
        print(f"Error: Error reading Excel: {e}")
        return False
        
    # Check for missing columns
    missing = [c for c in mapping.keys() if c not in df.columns]
    if missing:
        print(f"Error: Missing columns: {missing}")
        return False
        
    # 4. Clean and Transform
    df_clean = df[list(mapping.keys())].rename(columns=mapping).copy()
    
    # Remove 'Total' row
    df_clean = df_clean[df_clean['employee_id'] != 'Total']
    
    # Add weekending
    df_clean['weekending'] = weekending
    
    # Handle composite employee name for FN FT
    if is_fn_ft:
        df_clean['employee'] = df_clean['first_name'] + ' ' + df_clean['surname']
    
    # Handle site code for details-based reports
    if not is_fn_ft:
        df_clean['site'] = df_clean['site'].apply(transform_site)
    
    # Reorder columns
    df_clean = df_clean[cols]
    
    # 5. Inject into Database
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # 5a. Idempotency: Delete existing records
        cur.execute(f"DELETE FROM {target_table} WHERE weekending = %s", (weekending,))
        rows_deleted = cur.rowcount
        if rows_deleted > 0:
            print(f"  Cleared {rows_deleted} existing rows for weekending {weekending} in {target_table}.")
        
        # 5b. Prepare buffer for COPY
        buffer = StringIO()
        df_clean.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        cur.copy_from(buffer, target_table, sep=',', columns=cols)
        
        conn.commit()
        print(f"  Successfully injected {len(df_clean)} rows.")
        
        # 6. Archive file
        if not os.path.exists(ARCHIVE_DIR):
            os.makedirs(ARCHIVE_DIR)
            
        dest_path = os.path.join(ARCHIVE_DIR, filename)
        if os.path.exists(dest_path):
            name, ext = os.path.splitext(filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest_path = os.path.join(ARCHIVE_DIR, f"{name}_{timestamp}{ext}")
            
        shutil.move(file_path, dest_path)
        print(f"  Archived to: {dest_path}")
        return True
        
    except Exception as e:
        print(f"Error: Database error: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    if not os.path.exists(INPUT_DIR):
        print(f"Warning: Input directory '{INPUT_DIR}' does not exist.")
        return

    # Check all patterns
    files = []
    for pattern in [PATTERN_CASUAL, PATTERN_WEEKLY, PATTERN_FN_FT]:
        found = glob.glob(os.path.join(INPUT_DIR, pattern))
        if found:
            files.extend(found)
    
    if not files:
        print(f"No audit files found in {INPUT_DIR}")
        return
        
    for f in files:
        process_file(f)

if __name__ == "__main__":
    main()
