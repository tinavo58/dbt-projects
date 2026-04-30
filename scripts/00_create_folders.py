import pandas as pd
import os

from datetime import date, timedelta, datetime
from pathlib import Path
from dotenv import load_dotenv; load_dotenv(override=True)


def get_week_ending() -> str:
    """return WEEK ENDING date (Sunday) in YYYY-MM-DD format"""
    today_ = date.today()
    week_ending = today_ - timedelta(days=(today_.isoweekday() % 7))

    return week_ending.strftime('%Y-%m-%d')


def convert_datetime_to_diff_format(date_str) -> str:
    """Convert date string from YYYY-MM-DD to DD.MM.YYYY"""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return date_obj.strftime("%d.%m.%Y")


def create_folders(base_path: str, subfolder_names: list) -> None:
    """
    Create a folder and its subfolders.
    Args:
        base_path (str): The base directory where the folder and subfolders will be created.
    """

    folder_path = Path(base_path)
    folder_path.mkdir(parents=True, exist_ok=True)

    # Create subfolders and Final inside each
    for subfolder in subfolder_names:
        subfolder_path = folder_path / subfolder
        subfolder_path.mkdir(parents=True, exist_ok=True)

        # Create 'Final' folder inside each subfolder
        if subfolder != "z_TimeSheets and Others":
            ## create 'Final' folder inside each subfolder except 'z_TimeSheets and Others
            final_folder_path = subfolder_path / 'Final'
            final_folder_path.mkdir(parents=True, exist_ok=True)


def generate_folder_name():
    # Use PROJECT_ROOT (one level up from scripts) to resolve paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    fy_file_env = os.getenv('FY_FILE_PATH')
    if fy_file_env:
        # If it's an absolute path, use it; otherwise, relative to project_root
        if os.path.isabs(fy_file_env):
            calendarFilePath = Path(fy_file_env)
        else:
            calendarFilePath = Path(project_root) / fy_file_env
    else:
        # Default fallback
        calendarFilePath = Path(project_root) / 'data/input/AccountingCalendar.xlsx'

    if not calendarFilePath.exists():
        print(f"Error: Accounting Calendar file not found at {calendarFilePath}")
        return

    df = pd.read_excel(calendarFilePath, sheet_name='FY26', skiprows=2, usecols="B,D:F", index_col=0)
    weDateType = get_week_ending()
    weStringFormat = convert_datetime_to_diff_format(weDateType)

    try:
        period = df.loc[weDateType]
    except KeyError:
        print(f"Error: Date {weDateType} not found in {calendarFilePath}")
        return

    # Folder created in project_root/imported - using imported folder
    folderName = os.path.join(project_root, "imported", f"{period['idx']} WE {weStringFormat}")

    # ---- when accg period file is not available ----
    # no Accounting period file, so use the current week ending date
    # weStringFormat = convert_datetime_to_diff_format(get_week_ending())
    # folderName = f"output/WE {weStringFormat}" # parent folder

    """
    parent folder
    then create list of subfolders
    """

    subfoldersList = [
        f"Weekly Casual WE {weStringFormat}",
        f"Weekly FT WE {weStringFormat}",
        # f"Weekly WC WE {weStringFormat}", # WC not required for EH atm
        "z_TimeSheets and Others",
    ]

    if period['FN payrun'] == 1:
        subfoldersList.append(f"FN {weStringFormat}")


    create_folders(folderName, subfoldersList)
    print(f'Folders created successfully at: {folderName}')


if __name__ == "__main__":
    generate_folder_name()
