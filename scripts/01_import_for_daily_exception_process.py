import os
import glob
import shutil
import psycopg2
import pandas as pd
import subprocess
from io import StringIO
from dotenv import load_dotenv; load_dotenv(override=True)
from datetime import datetime, timedelta

# --- CONFIGURATION FROM .ENV ---
DATABASE_URL = os.getenv("DATABASE_URL")
DOWNLOAD_PATH = os.getenv("DOWNLOAD_PATH")
INPUT_DIR = DOWNLOAD_PATH if DOWNLOAD_PATH else "input"
# Archive should remain in the project folder
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE_DIR = os.path.join(PROJECT_ROOT, "imported")

class BaseImporter:
    def __init__(self, table_name, file_pattern):
        self.table_name = table_name
        self.file_pattern = file_pattern
        self.conn = None

    def get_files(self):
        """Finds all files matching the pattern in the input folder."""
        if not os.path.exists(INPUT_DIR):
            print(f"⚠️ Input directory '{INPUT_DIR}' does not exist.")
            return []
        pattern = os.path.join(INPUT_DIR, self.file_pattern)
        return glob.glob(pattern)

    def move_to_archive(self, file_path):
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
        print(f"📦 Archived {file_name} to {ARCHIVE_DIR}/")

    def connect(self):
        return psycopg2.connect(DATABASE_URL)

    def run_import(self):
        files = self.get_files()
        if not files:
            print(f"🔍 No files found for {self.table_name} (Pattern: {self.file_pattern})")
            return

        for file_path in files:
            print(f"📂 Processing {self.table_name}: {os.path.basename(file_path)}")
            
            try:
                self.conn = self.connect()
                cur = self.conn.cursor()
                
                # Custom processing happens here
                success = self.process_and_inject(cur, file_path)
                
                if success:
                    self.conn.commit()
                    print(f"✅ {self.table_name} injection successful!")
                    self.move_to_archive(file_path)
                    self.post_import_action()
                else:
                    self.conn.rollback()

            except Exception as e:
                print(f"❌ Error importing {self.table_name}: {e}")
                if self.conn:
                    self.conn.rollback()
            finally:
                if self.conn:
                    self.conn.close()

    def process_and_inject(self, cur, file_path):
        """Override this in subclasses for specific logic."""
        raise NotImplementedError

    def post_import_action(self):
        """Optional hook for actions after a successful import."""
        pass

class DailyExceptionImporter(BaseImporter):
    def process_and_inject(self, cur, file_path):
        try:
            df = pd.read_csv(file_path)
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
            
            missing = [c for c in mapping.keys() if c not in df.columns]
            if missing:
                print(f"❌ Missing columns: {missing}")
                return False
                
            df_clean = df[list(mapping.keys())].rename(columns=mapping).copy()
            df_clean['ts_date'] = pd.to_datetime(df_clean['ts_date'], dayfirst=True).dt.date

            buffer = StringIO()
            df_clean.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            cur.copy_from(buffer, self.table_name, sep=',', columns=list(df_clean.columns))
            return True
        except Exception as e:
            print(f"❌ Processing error: {e}")
            return False

class ExceptionReportImporter(BaseImporter):
    def process_and_inject(self, cur, file_path):
        try:
            df = pd.read_csv(file_path)
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
            
            missing = [c for c in mapping.keys() if c not in df.columns]
            if missing:
                print(f"❌ Missing columns: {missing}")
                return False
                
            df_clean = df[list(mapping.keys())].rename(columns=mapping).copy()
            df_clean['ts_date'] = pd.to_datetime(df_clean['ts_date']).dt.date

            buffer = StringIO()
            df_clean.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            cur.copy_from(buffer, self.table_name, sep=',', columns=list(df_clean.columns))
            return True
        except Exception as e:
            print(f"❌ Processing error: {e}")
            return False

class LeaveRequestImporter(BaseImporter):
    def convert_duration(self, row):
        try:
            duration_str = str(row['duration']).lower()
            val = float(duration_str.split()[0])
            if 'day' in duration_str:
                return round(val * 7.6, 2)
            return val
        except:
            return 0.0

    def process_and_inject(self, cur, file_path):
        try:
            df = pd.read_csv(file_path)
            mapping = {
                'Personnel': 'staff',
                'From': 'start_date',
                'To': 'end_date',
                'Leave Category': 'leave_type',
                'Duration': 'duration'
            }
            
            missing_cols = [col for col in mapping.keys() if col not in df.columns]
            if missing_cols:
                print(f"❌ Missing columns: {missing_cols}")
                return False
                
            df_transformed = df[list(mapping.keys())].rename(columns=mapping).copy()
            df_transformed['duration'] = df_transformed.apply(self.convert_duration, axis=1)
            df_transformed['start_date'] = pd.to_datetime(df_transformed['start_date'], dayfirst=True).dt.date
            df_transformed['end_date'] = pd.to_datetime(df_transformed['end_date'], dayfirst=True).dt.date

            buffer = StringIO()
            df_transformed.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            cur.copy_from(buffer, self.table_name, sep=',', columns=list(df_transformed.columns))
            
            # Post-processing: Update seed CSV
            seeds_dir = 'seeds'
            os.makedirs(seeds_dir, exist_ok=True)
            output_path = os.path.join(seeds_dir, "leave_requests.csv")
            df_transformed.to_csv(output_path, index=False)
            print(f"  🚀 Updated seed file: {output_path}")
            
            return True
        except Exception as e:
            print(f"❌ Processing error: {e}")
            return False

    def post_import_action(self):
        try:
            print("  🏗️ Running 'dbt seed -s leave_requests'...")
            subprocess.run(["dbt", "seed", "-s", "leave_requests"], check=True)
            print("  ✅ dbt seed completed successfully.")
        except Exception as e:
            print(f"  ⚠️ Warning: dbt seed action failed: {e}")

class StaffWeeklyImporter(BaseImporter):
    def process_and_inject(self, cur, file_path):
        try:
            # Full Refresh - Clear table first
            cur.execute(f"TRUNCATE TABLE {self.table_name}")
            
            cols = "(employee_id, preferred_name, first_name, surname, date_of_birth, gender, residential_state, email_address, start_date, end_date, termination_reason, tags, has_working_holiday_visa, working_holiday_visa_country, job_title, pay_schedule, primary_pay_category, primary_location, rate, hours_per_week, leave_template, pay_rate_template, pay_condition_rule_set)"
            
            with open(file_path, 'r') as f:
                cur.copy_expert(f"COPY {self.table_name} {cols} FROM STDIN WITH (FORMAT CSV, HEADER true, DELIMITER ',')", f)
            return True
        except Exception as e:
            print(f"❌ Processing error: {e}")
            return False

def main():
    importers = [
        DailyExceptionImporter("daily_exception_export", os.getenv("PATTERN_DAILY_EXCEPTION", "DailyException_*.csv")),
        ExceptionReportImporter("exception_report", os.getenv("PATTERN_EXCEPTION_REPORT", "ExceptionReport*.csv")),
        LeaveRequestImporter("leave_requests", os.getenv("PATTERN_LEAVE_REQUESTS", "leave_taken_table*.csv")),
    ]
    
    print("🚦 Starting Payroll Import Automation...")
    for importer in importers:
        importer.run_import()
    print("🏁 All available files processed.")

if __name__ == "__main__":
    main()
