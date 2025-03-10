from processing.functions import find_column_headers, clean_value, find_location_components
import os
import re
import pandas as pd
import numpy as np


def main_recursive(df, debug=False, start=0, end=-1, debug_location=False, debug_header=False):
    """
    A recursive version of the main function for processing tables in a DataFrame.

    Args:
        df (pandas.DataFrame): Input DataFrame to process
        debug (bool): Enable debug output
        start (int): Starting index in DataFrame
        end (int): Ending index in DataFrame (-1 for end of DataFrame)
        debug_location (bool): Enable debug output for location components
        debug_header (bool): Enable debug output for header finding

    Returns:
        pandas.DataFrame: Processed data with structured columns
    """
    if end == -1:
        final_index = len(df)
    else:
        final_index = end

    # Initialize the new DataFrame
    new_df = pd.DataFrame(columns=['Province', 'City/Municipality', 'Barangay',
                                   'Street/Subdivision', 'Vicinity', 'Classification', 'ZV/SQM'])

    # Start recursion
    result_df, count = process_tables_recursive(
        df,
        new_df,
        start,
        final_index,
        None, None, None,  # Current province, city, barangay
        None, None, None, None,  # Previous values
        0,  # Table count
        False,  # Continuation flag
        debug, debug_location, debug_header
    )

    if debug:
        print(f"Total tables processed: {count}")

    return result_df


def process_tables_recursive(df, result_df, index, final_index,
                             current_province, current_city, current_barangay,
                             prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
                             count, continuation, debug, debug_location, debug_header):
    """
    Recursive function to process tables in the DataFrame.

    Args:
        df (pandas.DataFrame): Input DataFrame
        result_df (pandas.DataFrame): Output DataFrame being built
        index (int): Current position in the DataFrame
        final_index (int): End position to process
        current_province, current_city, current_barangay: Current location components
        prev_col1, prev_vicinity, prev_classification, prev_zvsqm: Previous values
        count (int): Count of tables processed so far
        continuation (bool): Whether this table is a continuation of the previous
        debug, debug_location, debug_header (bool): Debug flags

    Returns:
        tuple: (result_df, count) - The updated DataFrame and table count
    """
    # Base case: end of DataFrame or reached final index
    if index >= final_index:
        return result_df, count

    PROXIMITY_WINDOW = 2  # Increased to accommodate different formats

    # Find location components
    current_province_new, current_city_new, current_barangay_new, index = find_location_components(
        df, index, proximity_window=PROXIMITY_WINDOW, debug=debug_location)

    found_components = any([current_province_new, current_city_new, current_barangay_new])
    if found_components and debug:
        print(f"Location components found: {current_province_new}, {current_city_new}, {current_barangay_new}")

    # Attempt to find headers starting from the last matched index
    found_headers, header_indices, new_index = find_column_headers(df, index, debug=debug_header)
    if debug:
        print(f"Column headers found: {header_indices}")

    # If we found both headers and location components
    if found_headers and found_components:
        # Update continuation flag
        if current_province_new == current_province:
            continuation = True
        else:
            continuation = False

        # Update current location
        current_province = current_province_new if current_province_new else current_province
        current_city = current_city_new if current_city_new else current_city
        current_barangay = current_barangay_new if current_barangay_new else current_barangay

        # Move index to after headers
        index = new_index

        # Increment table count
        count += 1
        if debug:
            print(f'Processing table {count}\n')

        # Process the data rows
        result = process_data_rows_recursive(
            df, result_df, index, final_index,
            current_province, current_city, current_barangay,
            header_indices,
            prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
            continuation,
            debug, debug_location, debug_header
        )

        # Unpack the result
        result_df = result[0]
        index = result[1]
        new_prev_col1 = result[2]
        new_prev_vicinity = result[3]
        new_prev_classification = result[4]
        new_prev_zvsqm = result[5]
        found_new_table = result[6]

        # If a new table was found, handle it
        if found_new_table:
            # Get the new table information
            new_province = result[7]
            new_city = result[8]
            new_barangay = result[9]
            new_headers = result[10]

            # Update for the new table found within data rows
            if debug:
                print('\n' + '#' * 60)
                print('#' + ' ' * 58 + '#')
                print('#{:^58}#'.format(f'>>> PROCESSING TABLE {count + 1} <<<'))
                print('#' + ' ' * 58 + '#')
                print('#' * 60 + '\n')

            # Recursively process the new table
            return process_tables_recursive(
                df, result_df, index, final_index,
                new_province, new_city, new_barangay,
                new_prev_col1, new_prev_vicinity, new_prev_classification, new_prev_zvsqm,
                count + 1,  # Increment table count
                True if new_province == current_province else False,  # Set continuation flag
                debug, debug_location, debug_header
            )

        # Continue to next table (no new table was found within the data rows)
        return process_tables_recursive(
            df, result_df, index, final_index,
            current_province, current_city, current_barangay,
            new_prev_col1, new_prev_vicinity, new_prev_classification, new_prev_zvsqm,
            count, continuation,
            debug, debug_location, debug_header
        )
    else:
        # No headers or location found, move to next row
        return process_tables_recursive(
            df, result_df, index + 1, final_index,
            current_province, current_city, current_barangay,
            prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
            count, continuation,
            debug, debug_location, debug_header
        )


def process_data_rows_recursive(df, result_df, index, final_index,
                                current_province, current_city, current_barangay,
                                header_indices,
                                prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
                                continuation,
                                debug, debug_location, debug_header,
                                age=0, col1_holder=None, vicinity_holder=None, all_other_vicinity=None):
    """
    Recursively process data rows within a found table.

    Returns:
        tuple: (result_df, next_index, prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
                found_new_table, new_province, new_city, new_barangay, new_headers)
    """
    # Base cases
    MAX_AGE = 4
    if index >= final_index:
        return (result_df, index, prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
                False, None, None, None, None)

    if age >= MAX_AGE:
        return (result_df, index, prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
                False, None, None, None, None)

    # Get current row
    row = df.iloc[index]

    # Extract data using the header indices
    col1 = row.iloc[header_indices['street_name_index']]
    classification = row.iloc[header_indices['classification_index']]
    zv = row.iloc[header_indices['zv_sq_m_index']]

    # Handle vicinity (which could be a single column or two columns)
    vicinity = 'Test u should not see this pop up pls'
    if isinstance(header_indices['vicinity_index'], int):
        vicinity = row.iloc[header_indices['vicinity_index']]
    elif isinstance(header_indices['vicinity_index'], list):
        vicinity1 = str(row.iloc[header_indices['vicinity_index'][0]])
        vicinity2 = str(row.iloc[header_indices['vicinity_index'][1]])
        if vicinity1 == 'nan':
            vicinity = vicinity2
        elif vicinity2 == 'nan':
            vicinity = vicinity1
        else:
            vicinity = f"{vicinity1}, {vicinity2}"

    if debug:
        print(f"Data row at index {index}: {[col1, vicinity, classification, zv]}")

    # Check for new location components in the current row
    current_province_new_in_row, current_city_new_in_row, current_barangay_new_in_row, new_index_2 = find_location_components(
        df, index, debug=debug_location)

    # Check for new headers
    found_headers_in_row, header_indices_in_row, new_index_in_row = find_column_headers(df, new_index_2,
                                                                                        debug=debug_header)

    # Check if row has valid data
    combined_row = ''.join(
        map(str, row[[header_indices['classification_index'], header_indices['zv_sq_m_index']]].dropna())).strip()
    valid_data_row = clean_value(combined_row)

    if debug and any([current_province_new_in_row, current_city_new_in_row, current_barangay_new_in_row]):
        if current_province_new_in_row:
            print(f"Province found: {current_province_new_in_row}")
        if current_city_new_in_row:
            print(f"City/Municipality found: {current_city_new_in_row}")
        if current_barangay_new_in_row:
            print(f"Barangay found: {current_barangay_new_in_row}")
        if found_headers_in_row:
            print("Column headers found")
        print(f"Valid data row: {valid_data_row}")

    # Check if we found a new table
    if not valid_data_row and (any([current_province_new_in_row, current_city_new_in_row,
                                    current_barangay_new_in_row]) and found_headers_in_row):
        if debug:
            print(f"New location and headers found at index {index}. Ending current table and starting new table.")
            print(
                f"current_province: {current_province_new_in_row}, current_city: {current_city_new_in_row}, current_barangay: {current_barangay_new_in_row}")

        # Update location components
        next_province = current_province_new_in_row if current_province_new_in_row else current_province
        next_city = current_city_new_in_row if current_city_new_in_row else current_city
        next_barangay = current_barangay_new_in_row if current_barangay_new_in_row else current_barangay

        # Return signal to start a new table
        return (result_df, new_index_in_row, prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
                True, next_province, next_city, next_barangay, header_indices_in_row)

    # Check if the row contains valid data
    cleaned_row = clean_value(''.join(map(str, row.dropna())).strip())
    row_is_valid = (not ((pd.isnull(classification) or str(classification).strip() == '') and
                         (pd.isnull(zv) or str(zv).strip() == ''))) and str(cleaned_row).strip()

    if not row_is_valid:
        # Skip this row and increase age
        return process_data_rows_recursive(
            df, result_df, index + 1, final_index,
            current_province, current_city, current_barangay,
            header_indices,
            prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
            continuation,
            debug, debug_location, debug_header,
                           age + 1, col1_holder, vicinity_holder, all_other_vicinity
        )

    # Check if both classification and ZV/SQM are empty
    if (pd.isnull(classification) or str(classification).strip() == '') and (pd.isnull(zv) or str(zv).strip() == ''):
        # Skip this row and increase age
        return process_data_rows_recursive(
            df, result_df, index + 1, final_index,
            current_province, current_city, current_barangay,
            header_indices,
            prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
            continuation,
            debug, debug_location, debug_header,
                           age + 1, col1_holder, vicinity_holder, all_other_vicinity
        )

    if str(classification).strip().lower() == 'nan' and not str("ZV / SQ. M").replace('.', '', 1).isdigit():
        # Skip this row
        return process_data_rows_recursive(
            df, result_df, index + 1, final_index,
            current_province, current_city, current_barangay,
            header_indices,
            prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
            continuation,
            debug, debug_location, debug_header,
            age, col1_holder, vicinity_holder, all_other_vicinity
        )

    # Handle col1
    null_col1 = pd.isna(col1) or not str(col1).strip()
    if null_col1:
        if continuation:
            col1 = col1_holder if not (pd.isna(col1_holder) or not str(col1_holder).strip()) else prev_col1
        elif not (pd.isna(col1_holder) or not str(col1_holder).strip()):
            col1 = col1_holder
    else:
        col1_holder = col1

    # Check for "ALL OTHER" in col1
    if isinstance(col1, str):
        col1_stripped_upper = col1.strip().upper()
        is_all_other = col1_stripped_upper.startswith("ALL OTHER")
    else:
        col1_stripped_upper = ''
        is_all_other = False

    # Handle vicinity
    null_vicinity = pd.isna(vicinity) or not str(vicinity).strip()
    if null_vicinity:
        if continuation:
            if not (pd.isna(prev_col1) and pd.isna(col1)) and prev_col1 != col1:
                vicinity_holder = vicinity
            else:
                vicinity = vicinity_holder if not (
                        pd.isna(vicinity_holder) or not str(vicinity_holder).strip()) else prev_vicinity
        elif not (pd.isna(vicinity_holder) or not str(vicinity_holder).strip()):
            if not (pd.isna(prev_col1) and pd.isna(col1)) and prev_col1 != col1:
                vicinity_holder = vicinity
            else:
                vicinity = vicinity_holder
    else:
        vicinity_holder = vicinity

    # 'ALL OTHER' logic
    if is_all_other:
        if not null_vicinity:
            all_other_vicinity = vicinity
        if all_other_vicinity:
            vicinity = all_other_vicinity
        else:
            vicinity = ''
            if debug:
                print(f"'col1' starts with 'ALL OTHER'. Setting 'vicinity' to blank.")
    else:
        all_other_vicinity = None

    # Check for dash strings
    def is_dash_string(var):
        return isinstance(var, str) and re.fullmatch(r"\-+", var) is not None

    matches = sum(is_dash_string(var) for var in [col1, vicinity, classification, zv])
    if matches >= 3:
        # Skip this row and increase age
        return process_data_rows_recursive(
            df, result_df, index + 1, final_index,
            current_province, current_city, current_barangay,
            header_indices,
            prev_col1, prev_vicinity, prev_classification, prev_zvsqm,
            continuation,
            debug, debug_location, debug_header,
                           age + 1, col1_holder, vicinity_holder, all_other_vicinity
        )

    # Append to result DataFrame
    result_df.loc[len(result_df)] = [
        current_province,
        current_city,
        current_barangay,
        clean_value(col1, feature=True),
        clean_value(vicinity, feature=True),
        clean_value(classification, feature=True),
        clean_value(zv, feature=True)
    ]

    # Update previous values
    new_prev_col1 = col1
    new_prev_vicinity = vicinity
    new_prev_classification = classification
    new_prev_zvsqm = zv

    if debug:
        print(result_df.loc[len(result_df) - 1])
        print("\n-------\n")

    # Continue with next row (age reset to 0)
    return process_data_rows_recursive(
        df, result_df, index + 1, final_index,
        current_province, current_city, current_barangay,
        header_indices,
        new_prev_col1, new_prev_vicinity, new_prev_classification, new_prev_zvsqm,
        continuation,
        debug, debug_location, debug_header,
        0, col1_holder, vicinity_holder, all_other_vicinity
    )
