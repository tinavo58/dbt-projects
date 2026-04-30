import os
import re
import glob
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)
DATABASE_URL = os.getenv("DATABASE_URL")
DOWNLOAD_PATH = os.getenv("DOWNLOAD_PATH")
INPUT_DIR = DOWNLOAD_PATH if DOWNLOAD_PATH else "input"
PATTERN_PAY_CATEGORIES = os.getenv("PATTERN_PAY_CATEGORIES", "PayCategories*.csv")

def resolve_table_name(filename):
    """Derive table name from filename date range, e.g. PayCategories20230701-20240630.csv -> eh_pay_categories_report_fy24."""
    match = re.search(r'(\d{4})\d{4}-(\d{4})\d{4}', filename)
    if match:
        end_year = match.group(2)
        fy = f"fy{end_year[2:]}"
        return f"eh_pay_categories_report_{fy}"
    return None

COLUMN_MAPPING = {
    'Pay Category': 'pay_category',
    'PaymentSummaryClassification': 'payment_summary_classification',
    'Pay Run': 'pay_run',
    'Date Paid': 'date_paid',
    'Employee Id': 'employee_id',
    'First Name': 'first_name',
    'Surname': 'surname',
    'Location': 'location',
    'Units': 'units',
    'Rate': 'rate',
    'Amount': 'amount',
    'SG Super': 'sg_super'
}

def clean_data(df):
    """Subsets, renames, and formats the dataframe."""
    # Keep only required columns
    df = df[list(COLUMN_MAPPING.keys())].rename(columns=COLUMN_MAPPING)
    
    # Format dates
    df['date_paid'] = pd.to_datetime(df['date_paid'], errors='coerce').dt.date
    
    # Ensure numeric types
    numeric_cols = ['units', 'rate', 'amount', 'sg_super']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
    
    # Ensure employee_id is int (or string if it has leading zeros, but typically int)
    df['employee_id'] = pd.to_numeric(df['employee_id'], errors='coerce').fillna(0).astype(int)
    
    return df

def ingest_to_db(df, table_name):
    """Ingests the dataframe into the Postgres database."""
    print(f"Ingesting data into table: {table_name}...")
    engine = create_engine(DATABASE_URL)
    
    try:
        # Use if_exists='replace' to create or overwrite the table
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Successfully ingested {len(df)} rows into {table_name}.")
    except Exception as e:
        print(f"Error: Error during ingestion: {e}")
    finally:
        engine.dispose()

def main():
    if not DATABASE_URL:
        print("Error: DATABASE_URL not found in .env file.")
        return

    if not os.path.exists(INPUT_DIR):
        print(f"Warning: Input directory '{INPUT_DIR}' does not exist.")
        return

    files = glob.glob(os.path.join(INPUT_DIR, PATTERN_PAY_CATEGORIES))
    if not files:
        print(f"No files found in {INPUT_DIR} matching {PATTERN_PAY_CATEGORIES}")
        return

    for file_path in files:
        filename = os.path.basename(file_path)
        table_name = resolve_table_name(filename)
        if not table_name:
            print(f"Warning: Could not derive table name from '{filename}'. Skipping...")
            continue

        print(f"\nProcessing file: {file_path} -> {table_name}")
        try:
            df = pd.read_csv(file_path)
            df_clean = clean_data(df)
            ingest_to_db(df_clean, table_name)
        except Exception as e:
            print(f"Error: Error processing {file_path}: {e}")

if __name__ == "__main__":
    main()
