# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

dbt project for payroll reconciliation and exception reporting at eStore Logistics. Compares data between Deputy (timesheets/rostering), Employment Hero (payroll/HR), and internal records to identify discrepancies in hours, pay, and staff records.

**Database:** PostgreSQL (local, `estore` database on localhost:5432)
**Profile:** `dbt_payroll`
**Materialization:** All models default to views

## Commands

```bash
# Run all models
dbt run

# Run a specific model
dbt run --select fct_2a_payroll_variance_casual

# Run all models in a domain
dbt run --select payroll

# Run tests
dbt test

# Run a single test
dbt test --select assert_variance_weekly

# Load seed data
dbt seed

# Delete latest week of timesheets before re-import
dbt run-operation delete_latest_week_ts
```

### Python scripts (run from project root)

Numbered scripts form an ordered workflow for importing data:
- `00` creates folder structure
- `01` updates staff tables from Deputy/Employment Hero exports
- `02` imports daily exception CSVs and leave requests
- `03` converts timesheet exports for import
- `04` converts pay run audit XLSX reports to CSV
- `05` exports summaries to Google Sheets
- `06` imports pay categories

Scripts read `DATABASE_URL` and file patterns from `.env`. Input files land in the `input/` or `DOWNLOAD_PATH` directory and get archived to `imported/` after processing.

## Architecture

### Schema layout (via `generate_schema_name` macro)

Models write to bare schema names (no prefix), not `<target_schema>_<custom_schema>`:
- `staging` — timesheet prep and import validation
- `payroll` — pay run variance analysis (casual, weekly, fortnightly)
- `daily_exception` — shift exception checks from Deputy
- `staff` — staff record discrepancies between systems
- `super` — quarterly superannuation reconciliation

### Source systems

All sources are defined under `public` schema:
- **Deputy:** `deputy_ts` (timesheets), `deputy_staff`, `daily_exception_export`, `exception_report`, `leave_requests`
- **Employment Hero:** `eh_staff`, `payruns_cas`, `payruns_weekly_ft`, `payruns_fortnightly`, `pay_categories_report`

### Model naming conventions

- `int_` — intermediate transformations (not for direct consumption)
- `fct_` — fact models producing reconciliation/check results
- `rpt_` — report-level output (e.g., `rpt_quarterly_super_check`)
- Numbered prefixes on `fct_` models indicate workflow order (e.g., `fct_1a_`, `fct_2a_`)

### Key seeds

Seeds provide reference data and current-period imports (gitignored — loaded separately):
- `payroll_export` — current Deputy timesheet export (referenced heavily by payroll models)
- `dim_pay_categories` — pay category dimension table
- `casual_staff_with_dob` — used for birthday leave checks
- `quarterly_super`, `quarterly_pay_categories` — quarterly super reconciliation inputs

### Test pattern

Tests are assertion-based: they select from a model and **pass when zero rows are returned**. A row in the result means a discrepancy was found.
