import os
import re
import pandas as pd
import numpy as np


def extract_rdo_number(filename):
    try:
        # Use regular expressions to find the numeric part of the RDO number
        match = re.search(r'RDO No\. (\d+)\w? - (.+)\.?(?:xls|xlsx)?', filename, re.IGNORECASE)
        if match:
            return int(match.group(1))  # Extract the number and convert to integer
        else:
            return float('inf')
    except (ValueError, IndexError) as e:
        print(f"Error processing filename: {filename} - {e}")
        return float('inf')


def xls_to_df(filename, base_dir="data/", full_path=None):
    """
    Convert an Excel file to a pandas DataFrame.

    Args:
        filename (str): The Excel filename to convert
        base_dir (str): The base directory containing the Excel files

    Returns:
        tuple: (DataFrame, sheet_name) containing the data and sheet name used, or (None, None) if error
    """
    if not full_path:
        filepath = os.path.join(base_dir, filename)
    else:
        filepath = full_path

    try:
        # Check file extension and specify engine if necessary
        if filename.lower().endswith('.xls'):
            excel_file = pd.ExcelFile(filepath, engine='xlrd')  # Use xlrd for .xls files
        else:
            excel_file = pd.ExcelFile(filepath, engine='openpyxl')  # Use openpyxl for .xlsx files

        sheet_names = excel_file.sheet_names

        # Sort the sheet names if they follow the 'Sheet' naming pattern
        sheet_names = sorted([name for name in sheet_names if name.strip().lower().startswith('sheet')],
                             key=lambda name: int(re.search(r'\d+', name).group()))

        # Select the last sheet that matches the pattern
        if sheet_names:
            last_sheet_name = sheet_names[-1]
            df = pd.read_excel(filepath, sheet_name=last_sheet_name, header=None)
            return df, last_sheet_name
        else:
            print(f"No matching sheets found in {filename}")
            return None, None
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return None, None


def clean_value(value, feature=False):
    try:
        float_value = float(value)
        return round(float_value, 3)
    except (ValueError, TypeError):
        value = str(value)
        if value == 'nan':
            return ''
        if value is not None:
            value = re.sub(r"^\s*:\s*", "", value.strip())
            if not feature:
                value = re.sub(r"(D\.?\s*O\s*\.?\s*No|Effec(?:t)?ivity Date)\s*.*", "", value,
                               flags=re.IGNORECASE).strip()
            value = re.sub(r'^no\.\s*\d+\s*-\s*', '', value, flags=re.IGNORECASE).strip()
            value = re.sub(
                r"\s*-*\s*(\s*\(cont\s*\.\)|(?:\()?\s*continued\s*(?:\)?)|(?:\()?\s*continuation\s*(?:\))?|(?:\()?\s*continaution\s*(?:\))?|\s*revised).*",
                "", value, flags=re.IGNORECASE).strip()
            value = re.sub(r'[\s_]+$', '', value)
            return value
        return value


def extract_value(pattern, text):
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    else:
        return None


def find_column_headers(df, index, proximity_window=3, debug=False):
    import re

    headers = {
        'street_name_index': None,
        'vicinity_index': None,
        'classification_index': None,
        'zv_sq_m_index': None
    }

    headers_max_offset = {
        'street_name_index': -1,
        'vicinity_index': -1,
        'classification_index': -1,
        'zv_sq_m_index': -1
    }

    original_index = index
    column_texts = {}
    extend_search = False
    offset = 0

    zv_pattern_holder = None
    zv_offset_holder = None

    classification_pattern_holder = None

    while offset < proximity_window:
        current_index = index + offset
        if current_index >= len(df):
            break

        row = df.iloc[current_index]

        for col_index, cell in enumerate(row):
            cell_value = str(cell)
            if col_index not in column_texts:
                column_texts[col_index] = cell_value
            else:
                column_texts[col_index] += ' ' + cell_value

        if debug:
            print(f"Row {current_index}: {column_texts}")

        # Check each column's combined text for header patterns
        for col_index, combined_text in column_texts.items():
            if headers['street_name_index'] is None:
                if re.search(
                        r"(S\s*T\s*R\s*E\s*E\s*T\s*N\s*A\s*M\s*E|"
                        r"S\s*U\s*B\s*D\s*I\s*V\s*I\s*S\s*I\s*O\s*N|"
                        r"C\s*O\s*N\s*D\s*O\s*M\s*I\s*N\s*I\s*U\s*M)",
                        combined_text, re.IGNORECASE):
                    headers['street_name_index'] = col_index
                    headers_max_offset['street_name_index'] = current_index - original_index
                    if debug:
                        print(f"max offset updated: {current_index}")

            if headers['vicinity_index'] is None:
                if re.search(r"V\s*I\s*C\s*I\s*N\s*I\s*T\s*Y", combined_text, re.IGNORECASE):
                    headers['vicinity_index'] = col_index
                    headers_max_offset['vicinity_index'] = current_index - original_index
                    if debug:
                        print(f"max offset updated: {current_index}")

            if headers['classification_index'] is None:
                if re.search(
                        r"CLASS(?:IFICATION)?|"
                        r"C\s*L\s*A\s*S\s*S\s*I\s*F\s*I\s*C\s*A\s*T\s*I\s*O\s*N",
                        combined_text, re.IGNORECASE | re.DOTALL):
                    headers['classification_index'] = col_index
                    headers_max_offset['classification_index'] = current_index - original_index
                    if debug:
                        print(f"max offset updated: {current_index}")
                    extend_search = True  # Flag to extend the search

            if headers['zv_sq_m_index'] is None or headers['zv_sq_m_index'] < col_index:
                zv_pattern = (
                    r"\d+(?:ST|ND|RD|TH)\s+(?:REVISION|Rev)(?:.*Z\.?V\.?.*SQ.*M\.?)?|"
                    r"(?:\d+(?:ST|ND|RD|TH)\s+REVISION|Rev\s+ZV\s+/?.*SQ\.?\s*M\.?)|"
                    r"(?:Z|2)\.?V\.?.*SQ.*M\.?|FINAL"
                )
                match = re.search(zv_pattern, combined_text, re.IGNORECASE)
                if match:
                    headers['zv_sq_m_index'] = col_index
                    headers_max_offset['zv_sq_m_index'] = current_index - original_index
                    if debug:
                        print(f"max offset updated: {current_index}")

                    if not zv_pattern_holder:  # if this is the first one
                        zv_pattern_holder = match
                        zv_offset_holder = offset
                        headers['zv_sq_m_index'] = None
                        extend_search = True  # extend the search
                    elif zv_pattern_holder == match:  # if new pattern is the same, get previous values
                        headers_max_offset['zv_sq_m_index'] = zv_offset_holder

        if extend_search:
            if debug:
                print("Extending search")
            offset -= 2
            index += 2
            extend_search = False

        offset += 1

    # for the classification in 3 diff
    if headers['zv_sq_m_index'] and headers['vicinity_index']:
        if headers['zv_sq_m_index'] - headers['vicinity_index'] == 4:
            if headers['classification_index'] - headers['vicinity_index'] == 1:
                headers['classification_index'] += 1

    # If all headers were found, determine the maximum offset used
    if all(value is not None for value in headers.values()):
        # if we have a dupe
        header_values = list(headers.values())
        if debug:
            print(f"Offset values: {headers_max_offset.values()}")
        if len(header_values) != len(set(header_values)):
            if debug:
                print(f"Duplicate header index at index {index}")
            return False, None, original_index

        max_offset_used = max(headers_max_offset.values())
        if debug:
            print(f"Headers found within proximity window up to row {original_index + max_offset_used}")
            print(f"Header indices: {headers}")
        return True, headers, original_index + max_offset_used
    else:
        if debug:
            print(f"Headers not found within proximity window starting at index {original_index}")
        return False, None, original_index


def find_location_components(df, index, proximity_window=3, current_province=None, current_city=None,
                             current_barangay=None, debug=False):
    if debug:
        print(f"\nRunning find_location_components")
    last_matched_index = index
    initial_index = index
    expecting_values = False  # Flag to indicate we are expecting values in subsequent rows after combined labels
    found_any = False  # Flag to check if any location component is found

    extend_search = False
    offset = 0

    province_index = None
    city_index = None
    barangay_index = None
    barangay_holder = None
    city_holder = None

    while offset < proximity_window:

        current_index = index + offset
        if current_index >= len(df):
            break
        current_row = df.iloc[current_index]
        combined_current_row = ''.join(map(str, current_row.dropna())).strip()
        non_null_cells = current_row.dropna().astype(str).tolist()

        if debug:
            print("\n")
            # print(f"Row {current_index}: {non_null_cells}")
            # print(f"Expeting values: {expecting_values}")
            print(f"Searching row: {offset + 1}/{proximity_window}")

        # Check if this row contains the combined labels
        if not expecting_values and any(
                re.search(r"PROVINCE\s*/\s*CITY\s*/\s*MUNICIPALITY\s*/\s*BARANGAYS", cell, re.IGNORECASE) for cell in
                non_null_cells):
            expecting_values = True
            if debug:
                print(f"Combined labels found at row {current_index}")

            # offset += 1
            continue  # Move to the next row to read values

        # If we're expecting values after combined labels
        if expecting_values:
            # Iterate over cells to find values starting with ":"
            for cell in non_null_cells:
                cell = cell.strip()
                if debug:
                    print(f"Cell: {non_null_cells}")

                if cell.startswith(":"):
                    value = cell.lstrip(":").strip()
                    if not current_province:
                        current_province = clean_value(value)
                        found_any = True
                        if debug:
                            print(f"Province found: {current_province}")
                    elif not current_city:
                        current_city = clean_value(value)
                        found_any = True
                        if debug:
                            print(f"City/Municipality found: {current_city}")
                    elif not current_barangay:
                        current_barangay = clean_value(value)
                        found_any = True
                        if debug:
                            print(f"Barangay found: {current_barangay}")
            last_matched_index = current_index
            # If all components have values (either found now or already had values), we can return
            # if (current_province and current_city and current_barangay) or offset == proximity_window - 1:
            if all([current_province and current_city and current_barangay]):
                return current_province, current_city, current_barangay, last_matched_index + 1
            if offset == proximity_window - 1:
                return current_province, current_city, current_barangay, initial_index

            offset += 1
            continue  # Continue to next row to find remaining components


        # Original logic for separate labels
        else:
            if combined_current_row.lower().startswith("district"):
                if debug:
                    print(f"Skipping row {current_index} as it starts with 'district'")
                offset += 1
                continue
            # Check for Province
            province = extract_value(r"Province\s*(?::|\s|of)?\s*(.*)", combined_current_row)
            if province:
                current_province = clean_value(province)
                found_any = True
                extend_search = True
                last_matched_index = initial_index = province_index = current_index
                if debug:
                    print(f"Province match found in row {current_index}: {current_province}")

            # Check for City/Municipality
            city = extract_value(
                r"(?:(?!City,)(?:City|Municipality))(?:\s*\/\s*(?:City|Municipality))?\s*[:\s]?\s*(.+)",
                combined_current_row)
            if city:
                current_city = clean_value(city)
                found_any = True
                extend_search = True
                last_matched_index = initial_index = city_index = current_index
                if debug:
                    print(f"City/Municipality match found in row {current_index}: {current_city}")

            # Check for Barangay/Zone
            barangay = extract_value(
                r"(?:Barangays|Zone|Barangay)(?:\s*\/\s*(?:Barangays|Zone|Barangay))?\s*[:\s]?\s*(.+)",
                combined_current_row)
            # Check if the extracted barangay value contains a phrase like "along barangay road"
            if barangay and re.search(r".*\s*(?:along\s*)?barangay.*road.*", combined_current_row, re.IGNORECASE):
                # print(f"Discarding match due to 'along barangay road' pattern: {barangay}")
                barangay = None
            if barangay:
                current_barangay = clean_value(barangay)
                found_any = True
                extend_search = True
                last_matched_index = initial_index = barangay_index = current_index
                if debug:
                    print(f"Barangay/Zone match found in row {current_index}: {current_barangay}")

            if extend_search:
                # print("Extending search")
                offset -= 2
                index += 2
                extend_search = False

                # If we've found any component, we can check if we've reached the proximity window or if all components are found
            if found_any and all([current_province and current_city and current_barangay]):
                # if barangay index is before province index, look for a province pa, and if we find, overwrite
                if barangay_index is not None and province_index is not None and barangay_index < province_index and not barangay_holder:
                    if debug:
                        print("Extending search for new baranagay")
                    barangay_holder = current_barangay
                    current_barangay = None
                    offset -= 1
                    index += 2
                    continue
                # Similarly, if city index is before province index, look for a province and overwrite
                if city_index is not None and province_index is not None and city_index < province_index and not city_holder:
                    if debug:
                        print("Extending search for new city")
                    city_holder = current_city
                    current_city = None
                    offset -= 1
                    index += 2
                    continue
                if debug:
                    print(f"Found all location components! Last matched index: {last_matched_index}")
                return current_province, current_city, current_barangay, last_matched_index + 1

            if offset == proximity_window - 1:
                if found_any:
                    return current_province, current_city, current_barangay, last_matched_index + 1
                if barangay_holder:
                    current_barangay = barangay_holder
                    return current_province, current_city, current_barangay, last_matched_index + 1
                if city_holder:
                    current_city = city_holder
                    return current_province, current_city, current_barangay, last_matched_index + 1
                return current_province, current_city, current_barangay, initial_index

            # if extend_search:
            #     # print("Extending search")
            #     offset -= 2
            #     index += 2
            #     extend_search = False

        offset += 1
        if not expecting_values and not found_any:
            break

    return current_province, current_city, current_barangay, last_matched_index


def main(df, debug=False, start=0, end=-1, debug_location=False, debug_header=False):
    if end == -1:
        final_index = len(df)
    else:
        final_index = end
    index = start
    count = 0
    new_df = pd.DataFrame(columns=['Province', 'City/Municipality', 'Barangay',
                                   'Street/Subdivision', 'Vicinity', 'Classification', 'ZV/SQM'])

    PROXIMITY_WINDOW = 3  # Increased to accommodate different formats

    #
    current_province = None
    current_city = None
    current_barangay = None
    header_indices = None

    continuation = False
    # prev is previous table, holder is local table
    prev_col1 = None
    prev_vicinity = None
    prev_classification = None
    prev_zvsqm = None

    # while index < len(df):
    while index < final_index:
        current_province_new, current_city_new, current_barangay_new, index = find_location_components(
            df, index, proximity_window=PROXIMITY_WINDOW, debug=debug_location)
        # Update current location components with any new values

        found_components = any([current_province_new, current_city_new, current_barangay_new])
        if found_components and debug:
            print(f"Location components found: {current_province_new}, {current_city_new}, {current_barangay_new}")

        # Attempt to find headers starting from the last matched index
        found_headers, header_indices_new, new_index = find_column_headers(df, index, debug=debug_header)
        if debug:
            print(f"Column headers found: {header_indices_new}")

        # if we (kinda) confident we have a table
        if found_headers and found_components:
            if current_province_new == current_province:
                continuation = True
            else:
                continuation = False
            current_province = current_province_new if current_province_new else current_province
            current_city = current_city_new if current_city_new else current_city
            current_barangay = current_barangay_new if current_barangay_new else current_barangay

            # Update header indices
            header_indices = header_indices_new
            index = new_index  # Move index to after headers

            # Start processing data rows
            count += 1
            if debug:
                print(f'Processing table {count}\n')

            age = 0
            MAX_AGE = 4
            col1_holder = None
            vicinity_holder = None

            all_other_vicinity = None

            while index < final_index and age < MAX_AGE:
                # TODO: Check the types of all variables because some NaN stuff and floats and inconsistent and yeah
                row = df.iloc[index]

                vicinity = 'Test u should not see this pop up pls'
                # Extract data using the header indices
                col1 = row.iloc[header_indices['street_name_index']]
                classification = row.iloc[header_indices['classification_index']]
                zv = row.iloc[header_indices['zv_sq_m_index']]

                # Check for double column
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
                    # print(f'vicinity header index: {header_indices["vicinity_index"]}')

                # Check for new location components in the current row
                current_province_new_in_row, current_city_new_in_row, current_barangay_new_in_row, new_index_2 = find_location_components(
                    df, index, proximity_window=PROXIMITY_WINDOW, debug=debug_location)
                # found_headers_in_row, header_indices_in_row, new_index_in_row = find_column_headers(df, index, debug=debug)

                # if col1 index is not zone/barangay pattern
                # if barangay index is before province index, look for a province pa, and if we find, overwrite
                found_headers_in_row, header_indices_in_row, new_index_in_row = find_column_headers(df, new_index_2,
                                                                                                    debug=debug_header)

                combined_row = ''.join(map(str, row[
                    [header_indices['classification_index'], header_indices['zv_sq_m_index']]].dropna())).strip()
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

                # TODO: revisit this condition for new table
                # print(f"Validity: {valid_data_row}")
                if not valid_data_row and (any([current_province_new_in_row, current_city_new_in_row,
                                                current_barangay_new_in_row]) and found_headers_in_row):
                    # End current table processing
                    if debug:
                        print(
                            f"New location and headers found at index {index}. Ending current table and starting new table.")
                        print(
                            f"current_province: {current_province_new_in_row}, current_city: {current_city_new_in_row}, current_barangay: {current_barangay_new_in_row}")
                    # Update location components
                    if current_province_new_in_row == current_province:
                        continuation = True
                    else:
                        continuation = False
                    current_province = current_province_new_in_row if current_province_new_in_row else current_province
                    current_city = current_city_new_in_row if current_city_new_in_row else current_city
                    current_barangay = current_barangay_new_in_row if current_barangay_new_in_row else current_barangay

                    # Update headers
                    header_indices = header_indices_in_row
                    index = new_index_in_row  # Move index to after headers

                    # Reset variables
                    age = 0
                    col1_holder = None
                    vicinity_holder = None
                    count += 1  # Increment table count
                    if debug:
                        print('\n' + '#' * 60)
                        print('#' + ' ' * 58 + '#')
                        print('#{:^58}#'.format(f'>>> PROCESSING TABLE {count} <<<'))
                        print('#' + ' ' * 58 + '#')
                        print('#' * 60 + '\n')

                    continue  # Start processing new table from updated index

                cleaned_row = clean_value(''.join(map(str, row.dropna())).strip())
                row_is_valid = (not ((pd.isnull(classification) or str(classification).strip() == '') and (
                        pd.isnull(zv) or str(zv).strip() == ''))) and str(cleaned_row).strip()
                if not row_is_valid:
                    index += 1
                    age += 1
                    continue

                    # Check if both classification and ZV/SQM are empty
                if (pd.isnull(classification) or str(classification).strip() == '') and (
                        pd.isnull(zv) or str(zv).strip() == ''):
                    index += 1
                    age += 1
                    continue

                if str(classification).strip().lower() == 'nan' and not str("ZV / SQ. M").replace('.', '', 1).isdigit():
                    index += 1
                    continue

                # Checking for empty col1
                null_col1 = pd.isna(col1) or not str(col1).strip()
                if null_col1:
                    if continuation:
                        col1 = col1_holder if not (pd.isna(col1_holder) or not str(col1_holder).strip()) else prev_col1
                    elif not (pd.isna(col1_holder) or not str(col1_holder).strip()):
                        col1 = col1_holder
                else:
                    col1_holder = col1

                if isinstance(col1, str):
                    col1_stripped_upper = col1.strip().upper()
                    is_all_other = col1_stripped_upper.startswith("ALL OTHER")
                else:
                    col1_stripped_upper = ''
                    is_all_other = False

                # Check if 'vicinity' is null or empty
                null_vicinity = pd.isna(vicinity) or not str(vicinity).strip()
                if null_vicinity:  # if vicinity is null
                    if continuation:  # if the table is a continuation
                        if not (pd.isna(prev_col1) and pd.isna(col1)) and prev_col1 != col1:  # if new col1
                            vicinity_holder = vicinity  # update the holder
                        else:
                            vicinity = vicinity_holder if not (
                                    pd.isna(vicinity_holder) or not str(vicinity_holder).strip()) else prev_vicinity
                    elif not (pd.isna(vicinity_holder) or not str(vicinity_holder).strip()):
                        if not (pd.isna(prev_col1) and pd.isna(col1)) and prev_col1 != col1:  # if new col1
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

                def is_dash_string(var):
                    return isinstance(var, str) and re.fullmatch(r"\-+", var) is not None

                matches = sum(is_dash_string(var) for var in [col1, vicinity, classification, zv])
                if matches >= 3:
                    index += 1
                    age += 1
                    continue

                # Append to new DataFrame
                # TODO: check if cleaning features is necessary
                new_df.loc[len(new_df)] = [
                    current_province,
                    current_city,
                    current_barangay,
                    clean_value(col1, feature=True),
                    clean_value(vicinity, feature=True),
                    clean_value(classification, feature=True),
                    clean_value(zv, feature=True)
                ]

                prev_col1 = col1
                prev_vicinity = vicinity
                prev_classification = classification
                prev_zvsqm = zv

                if debug:
                    print(new_df.loc[len(new_df) - 1])
                    print("\n-------\n")

                index += 1
                age = 0
            continue  # Proceed to next iteration of the main loop
        else:
            index += 1  # No headers found, move to the next row
    if debug:
        print(f"Total tables processed: {count}")
    return new_df
