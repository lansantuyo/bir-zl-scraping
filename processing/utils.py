import os
import re

from processing.functions import xls_to_df, main
from processing.test_functions import main_recursive


def process_excel_files(input_dir="data/", initial_version=1, use_recursive=False):
    """
    Process all Excel files in a directory and save results with auto-incrementing version and date.
    Include the sheet name in the output filename.

    Args:
        input_dir (str): Input directory containing Excel files
        initial_version (int): Initial version number to start checking from
        use_recursive (bool): Whether to use the recursive version of main

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
        output_dir = f"output_v{version}_{date_str}"
        if not os.path.exists(output_dir):
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
                normalized_filename = f"{filename}.xlsx"
            else:
                print(f"Unsupported file format for {excel}. Skipping...")
                skipped_count += 1
                continue

            # Include sheet name in output filename
            output_path = os.path.join(output_dir, f"{clean_sheet_name}_{normalized_filename}")
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
