import os
import sys
import subprocess
import argparse
import shutil
import psycopg2
from io import StringIO
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# Load environment variables
load_dotenv(override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
INPUT_DIR = "input"
ARCHIVE_DIR = "imported"
FILE_PATTERN = os.getenv("PATTERN_LEAVE_REQUESTS", "leave_taken_table*.csv")

def convert_duration(row):
    """
    Transforms Duration:
    - Extracts the numeric value.
    - If the duration string contains "day", multiplies by 7.6 to convert to hours.
    - This specifically ensures Birthday Leave (which is 1.0 day) becomes 7.6 hours.
    """
    try:
        duration_str = str(row['duration']).lower()
        # Extract numbers from strings like "1.0 day(s)" or "7.6 hour(s)"
        val = float(duration_str.split()[0])
        
        if 'day' in duration_str:
            return round(val * 7.6, 2)
        return val
    except:
        return 0.0

def process_file(file_path):
    print(f"📂 Processing: {os.path.basename(file_path)}")
    
    # 1. Load CSV
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return False
        
    # 2. Map Columns (Standard mapping for stg_leave_requests)
    mapping = {
        'Personnel': 'staff',
        'From': 'start_date',
        'To': 'end_date',
        'Leave Category': 'leave_type',
        'Duration': 'duration'
    }
    
    # Validate columns
    missing_cols = [col for col in mapping.keys() if col not in df.columns]
    if missing_cols:
        print(f"❌ Missing columns: {missing_cols}")
        return False
        
    # Select and rename
    df_transformed = df[list(mapping.keys())].rename(columns=mapping).copy()
    
    # Transform Duration
    df_transformed['duration'] = df_transformed.apply(convert_duration, axis=1)
    
    # Convert dates to ISO format
    try:
        df_transformed['start_date'] = pd.to_datetime(df_transformed['start_date'], dayfirst=True).dt.date
        df_transformed['end_date'] = pd.to_datetime(df_transformed['end_date'], dayfirst=True).dt.date
    except Exception as e:
        print(f"  Warning: Date conversion issue: {e}")

    # 3. Inject into Database
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Prepare buffer for COPY
        buffer = StringIO()
        df_transformed.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        cols = list(df_transformed.columns)
        cur.copy_from(buffer, 'leave_requests', sep=',', columns=cols)
        
        conn.commit()
        print(f"  ✅ Injected {len(df_transformed)} rows into database.")
        
        # 4. Also update seed CSV for reference (optional but helpful)
        seeds_dir = 'seeds'
        if not os.path.exists(seeds_dir):
            os.makedirs(seeds_dir)
        output_path = os.path.join(seeds_dir, "leave_requests.csv")
        df_transformed.to_csv(output_path, index=False)
        print(f"  🚀 Updated seed file: {output_path}")
        
        # 4.5 Trigger dbt seed to refresh the database table
        try:
            print("  🏗️ Running 'dbt seed -s leave_requests'...")
            subprocess.run(["dbt", "seed", "-s", "leave_requests"], check=True)
            print("  ✅ dbt seed completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"  ⚠️ Warning: dbt seed failed: {e}")
        except Exception as e:
            print(f"  ⚠️ Warning: Could not run dbt: {e}")

        # 5. Archive file
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
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Process leave requests CSV and prepare for dbt.')
    parser.add_argument('input_path', type=str, nargs='?', help='Optional: Path to a specific input CSV file')
    
    args = parser.parse_args()
    
    # Manual mode
    if args.input_path:
        if not os.path.exists(args.input_path):
            print(f"Error: Input file '{args.input_path}' not found.")
            sys.exit(1)
        process_file(args.input_path)
    # Automatic mode
    else:
        if not os.path.exists(INPUT_DIR):
            print(f"⚠️ Input directory '{INPUT_DIR}' does not exist.")
            return

        import glob
        pattern = os.path.join(INPUT_DIR, FILE_PATTERN)
        files = glob.glob(pattern)
        
        if not files:
            print(f"🔍 No leave request files found in {INPUT_DIR} matching {FILE_PATTERN}")
            return
            
        for f in files:
            process_file(f)

if __name__ == "__main__":
    main()
