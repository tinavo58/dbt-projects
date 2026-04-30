import pandas as pd
import re
import sys
import os
import argparse
import shutil

def to_snake_case(name):
    """
    Converts a string to snake_case, handling camelCase by splitting 
    between lowercase and uppercase letters, then replacing spaces
    and non-alphanumeric characters with underscores.
    """
    # Insert underscores between lowercase/digits and uppercase letters
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    # Replace non-alphanumeric characters with underscores
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    # Convert to lowercase
    name = name.lower()
    # Strip leading and trailing underscores
    name = name.strip('_')
    return name

def main():
    parser = argparse.ArgumentParser(description='Convert CSV headers to snake_case and save as a dbt seed.')
    parser.add_argument('input_path', type=str, help='Path to the input CSV file')
    parser.add_argument('output_name', type=str, help='Desired name for the output CSV file (without extension)')
    
    args = parser.parse_args()
    
    input_path = args.input_path
    output_name = args.output_name
    
    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' not found.")
        sys.exit(1)
        
    # Load the CSV
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)
        
    # Convert headers to snake_case
    original_columns = df.columns.tolist()
    new_columns = [to_snake_case(col) for col in original_columns]
    df.columns = new_columns
    
    # NEW: Automatically fix date formats for columns containing 'date' or 'birth'
    for col in df.columns:
        if 'date' in col or 'birth' in col:
            try:
                # Check for slashes to detect DD/MM/YYYY format vs YYYY-MM-DD
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if sample and isinstance(sample, str) and '/' in sample:
                    df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
                else:
                    df[col] = pd.to_datetime(df[col], dayfirst=False, errors='coerce').dt.strftime('%Y-%m-%d')
                print(f"  Fixed date format for column: '{col}'")
            except Exception as e:
                print(f"  Warning: Could not format column '{col}' as a date: {e}")
    
    # Ensure the seeds directory exists
    seeds_dir = 'seeds'
    if not os.path.exists(seeds_dir):
        os.makedirs(seeds_dir)
        
    output_path = os.path.join(seeds_dir, f"{output_name}.csv")
    
    # Save the new CSV
    try:
        df.to_csv(output_path, index=False)
        print(f"Successfully converted headers and saved to: {output_path}")
        print("\nColumn Mappings:")
        for old, new in zip(original_columns, new_columns):
            print(f"  '{old}' -> '{new}'")
            
        # Archive the original file
        archive_dir = 'imported'
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
            
        archive_path = os.path.join(archive_dir, os.path.basename(input_path))
        shutil.move(input_path, archive_path)
        print(f"\nOriginal file archived to: {archive_path}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
