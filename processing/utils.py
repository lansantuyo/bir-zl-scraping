import os
import re
import pandas as pd
import numpy as np
import argparse

from processing.functions import xls_to_df, main, clean_value
from processing.test_functions import main_recursive


def process_excel_files(input_dir="data/", base_output_dir="output_files", initial_version=1, use_recursive=False):
    """
    Process all Excel files in a directory and save results with auto-incrementing version and date.
    Include the sheet name in the output filename.

    Args:
        input_dir (str): Input directory containing Excel files
        initial_version (int): Initial version number to start checking from
        use_recursive (bool): Whether to use the recursive version of main
        base_output_dir (str): directory for all output dirs

    Returns:
        str: Path to the output directory where files were saved
    """
    import datetime

    # Create output directory with version number and date (DD_MM format)
    today = datetime.datetime.now()
    date_str = today.strftime("%d_%m")  # DD_MM format

    # Auto-increment version number if directory exists
    version = initial_version
    while True:
        output_dir = os.path.join(base_output_dir, f"output_v{version}_{date_str}")
        
        # Check if any folder with this version exists, regardless of date
        version_exists = False
        for existing_dir in os.listdir(base_output_dir):
            if existing_dir.startswith(f"output_v{version}_"):
                version_exists = True
                break
        
        if not version_exists:
            break
        
        version += 1

    print(f"Creating output directory with version {version}: {output_dir}")

    os.makedirs(output_dir, exist_ok=True)
    excel_files = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]

    processed_count = 0
    skipped_count = 0

    for excel in excel_files:
        print(f'Processing {excel}')
        df_result = xls_to_df(excel, base_dir=input_dir)
        df, sheet_name = df_result

        if df is not None:
            if use_recursive:
                processed = main_recursive(df)
            else:
                processed = main(df)

            # Split the filename and the extension
            filename, extension = os.path.splitext(excel)

            if extension.lower() in ['.xls', '.xlsx']:
                # Clean sheet name for filename (remove spaces, special chars)
                clean_sheet_name = re.sub(r'[^a-zA-Z0-9]', '_', sheet_name) if sheet_name else "Unknown"
                normalized_filename = f"{filename}"
            else:
                print(f"Unsupported file format for {excel}. Skipping...")
                skipped_count += 1
                continue

            # Include sheet name in output filename
            output_path = os.path.join(output_dir, f"{normalized_filename}_{clean_sheet_name}.xlsx")
            processed.to_excel(output_path, index=False)

            print(f'Processed file saved as: {output_path}')
            processed_count += 1
        else:
            print(f"Could not process {excel}. Skipping...")
            skipped_count += 1

    print(f"Processing complete: {processed_count} files processed, {skipped_count} files skipped")
    print(f"All files saved to directory: {output_dir}")

    return output_dir


def excel_to_df(filename, path, use_recursive=False):
    df_result = xls_to_df(filename, full_path=path)
    df, sheet_name = df_result

    print(f"Processing: {filename}, sheet: {sheet_name}")

    return df


def compare_excel_files(file1, file2, num_rows=3, verbose=False, full_diff=False, output_path=None):
    """
    Compare two Excel files or pandas DataFrames and identify rows that are different.
    When a difference is found, print the row index and the next few rows from each file.

    Args:
        file1 (str or pandas.DataFrame): Path to the first Excel file or a pandas DataFrame
        file2 (str or pandas.DataFrame): Path to the second Excel file or a pandas DataFrame
        num_rows (int, optional): Number of rows to display when a difference is found. Default is 3.
        verbose (bool, optional): Whether to print detailed information. Default is False.
        full_diff (bool, optional): Whether to continue checking after first difference. Default is False.
        output_path (str, optional): Path to save the comparison results. Default is None.
    """
    import pandas as pd

    # Function to clean values if needed
    def clean_value(value):
        # Add your cleaning logic here if needed
        return value

    # Read the Excel files or use provided DataFrames
    try:
        if isinstance(file1, str):
            print(f"Reading file: {file1}")
            df1 = pd.read_excel(file1)
        else:
            print("Using provided DataFrame for file 1")
            df1 = file1.copy()

        if isinstance(file2, str):
            print(f"Reading file: {file2}")
            df2 = pd.read_excel(file2)
        else:
            print("Using provided DataFrame for file 2")
            df2 = file2.copy()

        # Get file names or identifiers for reporting
        file1_name = file1 if isinstance(file1, str) else "DataFrame 1"
        file2_name = file2 if isinstance(file2, str) else "DataFrame 2"

        print(f"Comparing {file1_name} and {file2_name}...")

        # Apply any necessary transformations (customize as needed)
        if "ZV/SQM" in df2.columns:
            df2["ZV/SQM"] = df2["ZV/SQM"].map(clean_value)

    except Exception as e:
        print(f"Error reading data: {e}")
        return

    # Check if DataFrames have the same shape
    if df1.shape != df2.shape:
        print(f"Data sources have different dimensions:")
        print(f"Source 1: {df1.shape[0]} rows, {df1.shape[1]} columns")
        print(f"Source 2: {df2.shape[0]} rows, {df2.shape[1]} columns")

        # Continue with comparison of overlapping rows
        min_rows = min(df1.shape[0], df2.shape[0])
        print(f"Comparing only the first {min_rows} rows...")
        df1 = df1.iloc[:min_rows, :]
        df2 = df2.iloc[:min_rows, :]

    # Initialize list to store comparison results
    comparison_results = []

    # Compare the DataFrames row by row
    differences_found = False
    for idx in range(len(df1)):
        if not df1.iloc[idx].equals(df2.iloc[idx]):
            differences_found = True

            # Print the row index where difference was found
            print(f"\n===== Difference found at row {idx} =====")

            # Get the next few rows or remaining rows if less than requested
            rows_to_display = min(num_rows, len(df1) - idx)

            # Create comparison for this difference
            diff_info = {
                "row_index": idx,
                "file1_rows": df1.iloc[idx:idx + rows_to_display].to_dict('records'),
                "file2_rows": df2.iloc[idx:idx + rows_to_display].to_dict('records')
            }
            comparison_results.append(diff_info)

            if verbose:
                # Print the difference
                print(f"\nSource 1 ({file1_name}) rows {idx} to {idx + rows_to_display - 1}:")
                print(df1.iloc[idx:idx + rows_to_display].to_string())

                print(f"\nSource 2 ({file2_name}) rows {idx} to {idx + rows_to_display - 1}:")
                print(df2.iloc[idx:idx + rows_to_display].to_string())

            # Highlight specific differences in the first different row
            different_columns = []
            for col in df1.columns:
                if col in df2.columns and df1.iloc[idx][col] != df2.iloc[idx][col]:
                    different_columns.append(col)

            # Process differences to identify "real" differences
            real_differences = []

            # Check each column with differences
            for col in different_columns:
                # Check for case differences in string columns
                if isinstance(df1.iloc[idx][col], str) and isinstance(df2.iloc[idx][col], str):
                    if df1.iloc[idx][col].lower() == df2.iloc[idx][col].lower():
                        if verbose:
                            print(f"Column '{col}': Only case difference")
                        continue  # This column has only case difference

                # Check for NaN/None differences
                elif pd.isna(df1.iloc[idx][col]) and pd.isna(df2.iloc[idx][col]):
                    if verbose:
                        print(f"Column '{col}': Both values are missing (NaN/None)")
                    continue  # This column has only NaN/None difference

                # If we get here, this is a real difference
                real_differences.append(col)

            # If no real differences remain, skip this row
            if not real_differences:
                print("Only case differences or equivalent missing values found - verbose=True for more info")
                continue  # Skip this difference and continue checking other rows
            else:
                # Replace different_columns with real_differences for reporting
                different_columns = real_differences

            if different_columns:
                print("\nSpecific differences in row", idx, ":")
                for col in different_columns:
                    print(f"Column '{col}':")
                    print(f"  Source 1: {df1.iloc[idx][col]}")
                    print(f"  Source 2: {df2.iloc[idx][col]}")

            if not full_diff:
                break

    if not differences_found:
        print("No differences found. The data sources are identical.")

    # Save results to output file if specified
    if output_path and comparison_results:
        with open(output_path, 'w') as f:
            f.write(f"Comparison between {file1_name} and {file2_name}\n\n")
            for diff in comparison_results:
                idx = diff["row_index"]
                f.write(f"Difference at row {idx}:\n")

                f.write(f"\nSource 1 rows {idx} to {idx + len(diff['file1_rows']) - 1}:\n")
                f.write(pd.DataFrame(diff["file1_rows"]).to_string())

                f.write(f"\nSource 2 rows {idx} to {idx + len(diff['file2_rows']) - 1}:\n")
                f.write(pd.DataFrame(diff["file2_rows"]).to_string())

                f.write("\n" + "=" * 50 + "\n")

        print(f"\nComparison results saved to {output_path}")

    return comparison_results