import pandas as pd
import io
import os
import glob
import shutil
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# fetch DB
DB_CONN = os.getenv('DATABASE_URL')
if not DB_CONN:
    print("Error: Neither DATABASE_URL nor CONN_STRING found in environment or .env file.")
    # You might want to exit here if this is critical
else:
    engine = create_engine(DB_CONN)

# Archive configuration
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "imported")

def move_to_archive(file_path):
    """Moves the file to an 'imported' subdirectory."""
    if not os.path.exists(ARCHIVE_DIR):
        os.makedirs(ARCHIVE_DIR)
        
    file_name = os.path.basename(file_path)
    dest_path = os.path.join(ARCHIVE_DIR, file_name)
    
    if os.path.exists(dest_path):
        name, ext = os.path.splitext(file_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_path = os.path.join(ARCHIVE_DIR, f"{name}_{timestamp}{ext}")

    shutil.move(file_path, dest_path)
    print(f"Archived {file_name} to {ARCHIVE_DIR}/")

def fast_import_deputy(df, table_name):
    """Speeds up Deputy data import using Postgres COPY command."""
    with engine.begin() as conn:
        try:
            print(f"Truncating {table_name}...")
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))

            # Create buffer
            output = io.StringIO()
            df.to_csv(output, sep='|', header=False, index=False)
            output.seek(0)

            cols = ', '.join([f'"{k}"' for k in df.columns])

            print(f"Importing {len(df)} rows into {table_name}...")
            sql = f"COPY {table_name} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER '|', NULL '')"
            
            with conn.connection.cursor() as cursor:
                cursor.copy_expert(sql, output)

            print(f"Success! {table_name} updated.")

        except Exception as e:
            conn.rollback()
            print(f"Error updating {table_name}: {e}")

def update_deputy_staff():
    """Reads and cleans Deputy staff data, then imports it."""
    file_pattern = os.getenv('CSV_FILE_PATH_DEPUTY_DETAILS')
    if not file_pattern:
        print("Error: CSV_FILE_PATH_DEPUTY_DETAILS not set in .env")
        return

    files = glob.glob(file_pattern)
    if not files:
        print(f"Error: No files found matching pattern {file_pattern}")
        return

    df = pd.read_csv(
        files[0],
        usecols=['Preferred Name', 'Location Name', 'Role', 'Payroll ID', 'Library Award', 'Base Rate']
    )

    # Rename to match SQL columns
    df = df.rename(columns={
        'Payroll ID': 'payroll_id',
        'Preferred Name': 'staff',
        'Location Name': 'location_name',
        'Role': 'access_role',
        'Library Award': 'library_award',
        'Base Rate': 'base_rate'
    })

    # CLEANING: Handle the Payroll ID
    df['payroll_id'] = pd.to_numeric(df['payroll_id'], errors='coerce').astype('Int64')

    fast_import_deputy(df, 'deputy_staff')
    move_to_archive(files[0])

def clean_tags(val):
    """Converts 'Tag1|Tag2' string to a Python list for Postgres Arrays."""
    if pd.isna(val) or val == '':
        return []
    return str(val).split('|')

def update_eh_staff():
    """Reads and cleans Employment Hero staff data, then imports it."""

    file_pattern = os.getenv('EXCEL_FILE_PATH_EH_EMPLOYEE_FILE')
    table_name = os.getenv('EH_EMPLOYEE_TABLE_NAME')

    if not file_pattern or not table_name:
        print("Error: EH environment variables (EXCEL_FILE_PATH_EH_EMPLOYEE_FILE, EH_EMPLOYEE_TABLE_NAME) not set.")
        return

    files = glob.glob(file_pattern)
    if not files:
        print(f"Error: No files found matching EH pattern {file_pattern}")
        return
    
    excel_file = files[0]

    cols_mapping = {
        'EmployeeId': 'employee_id',
        'PreferredName': 'preferred_name',
        'FirstName': 'first_name',
        'Surname': 'surname',
        'DateOfBirth': 'date_of_birth',
        'Gender': 'gender',
        'ResidentialState': 'residential_state',
        'EmailAddress': 'email_address',
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        'TerminationReason': 'termination_reason',
        'Tags': 'tags',
        'HasApprovedWorkingHolidayVisa': 'has_working_holiday_visa',
        'WorkingHolidayVisaCountry': 'working_holiday_visa_country',
        'JobTitle': 'job_title',
        'PaySchedule': 'pay_schedule',
        'PrimaryPayCategory': 'primary_pay_category',
        'PrimaryLocation': 'primary_location',
        'Rate': 'rate',
        'HoursPerWeek': 'hours_per_week',
        'LeaveTemplate': 'leave_template',
        'PayRateTemplate': 'pay_rate_template',
        'PayConditionRuleSet': 'pay_condition_rule_set'
    }

    print(f"Reading {excel_file}...")
    try:
        df = pd.read_excel(excel_file, usecols=cols_mapping.keys())
    except Exception as e:
        print(f"Error reading EH Excel file: {e}")
        return

    df = df.rename(columns=cols_mapping)

    print("Cleaning EH data...")
    date_cols = ['date_of_birth', 'start_date', 'end_date']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=False)

    df['tags'] = df['tags'].apply(clean_tags)
    
    df['has_working_holiday_visa'] = df['has_working_holiday_visa'].astype(bool)

    num_cols = ['rate', 'hours_per_week']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    print(f"Importing {len(df)} rows into {table_name}...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"))
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=500)

    print(f"Success! {table_name} updated.")
    move_to_archive(excel_file)

if __name__ == "__main__":
    print("\n--- Starting Staff Table Updates ---")
    update_deputy_staff()
    print("-" * 40)
    update_eh_staff()
    print("--- All Staff Updates Completed ---\n")
