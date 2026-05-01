# dbt Model Documentation

This folder contains the dbt models for the payroll project, organized by domain.

---

## Daily Exception

Models for processing and analyzing payroll exceptions and staff work hours.

### Source Data
- **`daily_exception_export`**: Raw Deputy export with pay rule units/costs (used for Overtime).
- **`exception_report`**: Deputy report highlighting schedule deviations and missed breaks.
- **`leave_requests`**: Processed staff leave data (converted to hours).

### Data Ingestion
To update the data for these reports, run the following commands in order:

1.  **Import Core Data**:
    ```bash
    python3 scripts/01_import_daily_exception.py
    python3 scripts/02_import_exception_report.py
    ```
2.  **Process Leave & Seeds**:
    ```bash
    python3 scripts/03_prepare_leave_requests.py
    ```
    *(This script now automatically runs `dbt seed -s leave_requests` for you)*
3.  **Run dbt Models**:
    ```bash
    dbt run -s daily_exception
    ```

### Intermediate Models
- **`int_daily_exceptions_base`**: Filters the exception report to the latest week and normalises validation flags (e.g. auto-closed shifts).
- **`int_daily_exceptions_checks`**: Splits base exceptions into check categories (Auto Closed Shift, Min Engagement, Breaks).
- **`int_overtime_checks`**: Aggregates overtime hours and costs from the daily exception export by pay rule.

### Core Reports
- **`fct_daily_exception_summary`**: Weekly consolidated view of all exceptions (Auto-closed shifts, Min Engagement, Breaks) and Overtime costs.
- **`fct_permanent_staff_hours_check`**: Identifies FT staff working < 37 hours by reconciling timesheet hours and leave requests.

---

## Payroll

Models for payroll calculations, variance analysis, and timesheet imports.

### Source Data
- **`deputy_ts`**: Deputy timesheets data.
- **`payruns_cas`**: Casual payrun details from Employment Hero.
- **`payruns_weekly_ft`**: Weekly full-time payrun details from Employment Hero.
- **`payruns_fortnightly`**: Fortnightly payrun details from Employment Hero.
- **`pay_categories_report`**: Mapping and detail for pay categories.

### Data Ingestion
To update the data for these reports, run the following commands in order:

1.  **Process Timesheets**:
    ```bash
    python3 scripts/04_convert_timesheet_import_seeds.py
    ```
    *(This script now automatically runs `dbt seed -s payroll_export` for you)*
2.  **Process Casual PayRun**:
    ```bash
    python3 scripts/05_convert_audit_report_to_csv.py
    ```
    *(Processes Casual PayRun Audit Excel files and injects to `payruns_cas`)*
3.  **Run dbt Models**:
    ```bash
    dbt run -s payroll
    ```

### Intermediate Models
- **`int_payruns_casual_current`**: Filters casual payruns to the latest weekending.
- **`int_payruns_weekly_current`**: Filters weekly FT payruns to the latest weekending.
- **`int_pay_category_classifications`**: Classifies all pay categories into ORD, OT1.5, OT2.0, ALLOW, or WC.

### Core Reports
- **`fct_1a_daily_casual_hours_summary`**: Daily breakdown of casual staff hours with estimated ordinary/overtime split, compared against payrun data.
- **`fct_1b_daily_weekly_hours_summary`**: Daily breakdown of weekly FT staff hours with estimated ordinary/overtime split, compared against payrun data.
- **`fct_2a_payroll_variance_casual`**: Reconciles casual worked timesheets against processed payruns to find discrepancies.
- **`fct_2b_payroll_variance_weekly`**: Reconciles weekly FT worked timesheets against processed payruns to find discrepancies.
- **`fct_2b_payroll_variance_weekly_list_all`**: Lists all weekly FT payrun entries with current vs. previous period comparison.
- **`fct_2c_payroll_variance_fortnightly`**: Compares current vs. previous fortnightly payrun for variance detection.
- **`fct_check_birthday_leave`**: Validates that birthday leave was taken within 2 weeks of the staff member's actual birthday.

---

## Staging

Models for preparing and validating timesheets before payroll import.

### Core Reports
- **`fct_timesheets_to_import`**: Formats timesheets from the `payroll_export` seed that are ready to be imported into the payroll system.
- **`fct_timesheets_with_no_payroll_id`**: Identifies timesheets missing an employee ID (excluding labour hire staff), flagging records that cannot be imported.
- **`fct_dayworkers_afternoon_check`**: Flags non-shiftworker staff who have afternoon/evening timesheets that may need review.

---

## Staff

Models for managing staff metadata and alignment between different systems.

### Source Data
- **`eh_staff`**: Employment Hero staff data.
- **`deputy_staff`**: Deputy staff data.

### Core Reports
- **`fct_national_workforce_export`**: Cleaned and formatted staff data for workforce reporting.
- **`fct_staff_discrepancies`**: Highlights differences in staff data between Deputy and Employment Hero.
- **`fct_staff_grade_tags_check`**: Validates that staff have the correct pay grade tags in Deputy (e.g. Above Award for Grade 3/4).
- **`fct_labour_hire`**: Identifies labour hire staff in Deputy by filtering on non-standard access roles.

---

## Superannuation

Models for validating superannuation contributions against earnings.

### Source Data
- **`quarterly_super`**: Raw reported superannuation payments.
- **`quarterly_pay_categories`**: Raw payroll earnings data.
- **`dim_pay_categories`**: Dimension table defining which pay categories are superable.

### Intermediate Models
- **`int_superable_pay_categories`**: Filters for superable earnings including special rules (e.g. taxable Leave Loading).

### Core Reports
- **`rpt_quarterly_super_check`**: Reconciles reported superannuation against a calculated "Super Check" (Superable Gross x 12%, capped at $7,500/quarter).

---

## Architecture Note

The models are organized into **Domain Folders** (Daily Exception, Payroll, Staging, Staff, Super). Each folder contains the full lineage for that domain:
- **Intermediate (`int_...`)**: Complex joins and business logic transformations.
- **Marts/Fact/Report (`fct_...`, `rpt_...`)**: Final output tables used for reporting and analytics.
