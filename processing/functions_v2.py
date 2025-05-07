import os
import re
import pandas as pd



def extract_rdo_number(filename: str) -> float:
    """
    Extracts the RDO number from a filename.

    Args:
        filename: The filename string.

    Returns:
        The RDO number as an integer, or float('inf') if not found or error.
    """
    try:
        # Use regular expressions to find the numeric part of the RDO number
        # Pattern expects "RDO No. <number><optional_char> - <name>.xls(x)"
        match = re.search(r'RDO No\. (\d+)\w? - (.+)\.?(?:xls|xlsx)?', filename, re.IGNORECASE)
        if match:
            return int(match.group(1))  # Extract the number and convert to integer
        else:
            return float('inf')  # Return infinity if no match, for sorting purposes
    except (ValueError, IndexError) as e:
        print(f"Error processing filename for RDO number: {filename} - {e}")
        return float('inf')


def xls_to_df(filename: str, base_dir: str = "data/", full_path: str = None) -> tuple[pd.DataFrame | None, str | None]:
    """
    Convert an Excel file to a pandas DataFrame, selecting a specific sheet.

    It tries to find sheets named like "SheetX" and picks the one with the highest number.

    Args:
        filename: The Excel filename to convert.
        base_dir: The base directory containing the Excel files.
        full_path: Optional full path to the file, overrides base_dir and filename.

    Returns:
        A tuple: (DataFrame, sheet_name) containing the data and the name of the
        sheet used, or (None, None) if an error occurs or no suitable sheet is found.
    """
    filepath = full_path if full_path else os.path.join(base_dir, filename)

    try:
        # Determine engine based on file extension
        if filename.lower().endswith('.xls'):
            excel_file = pd.ExcelFile(filepath, engine='xlrd')
        elif filename.lower().endswith('.xlsx'):
            excel_file = pd.ExcelFile(filepath, engine='openpyxl')
        else:
            # Fallback or error for unsupported types, though ExcelFile might handle some
            excel_file = pd.ExcelFile(filepath)  # Rely on pandas default engine detection

        all_sheet_names = excel_file.sheet_names

        # Filter for sheets matching the "Sheet<number>" pattern and sort them
        relevant_sheet_names = sorted(
            [name for name in all_sheet_names if name.strip().lower().startswith('sheet')],
            key=lambda name: int(re.search(r'\d+', name).group()) if re.search(r'\d+', name) else -1
        )

        if relevant_sheet_names:
            selected_sheet_name = relevant_sheet_names[-1]  # Select the last sheet in the sorted list
            df = pd.read_excel(excel_file, sheet_name=selected_sheet_name, header=None)
            return df, selected_sheet_name
        else:
            print(f"No matching sheets (e.g., 'Sheet1') found in {filename}")
            return None, None

    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return None, None


def clean_value(value, feature: bool = False) -> str | float:
    """
    Cleans a given value. Converts to float and rounds if possible.
    Otherwise, performs various string cleaning operations.

    Args:
        value: The value to clean.
        feature: If True, less aggressive cleaning is applied (omits D.O/Effectivity date removal).

    Returns:
        Cleaned value, either as a float or string.
    """
    try:
        float_value = float(value)
        return round(float_value, 3)
    except (ValueError, TypeError):
        # Ensure value is a string for subsequent operations
        value_str = str(value)

        if value_str.lower() == 'nan':  # Handle 'nan' string
            return ''

        cleaned_str = value_str.strip()

        # Remove leading colon with optional spaces
        cleaned_str = re.sub(r"^\s*:\s*", "", cleaned_str)

        if not feature:
            # Remove "D.O No." or "Effectivity Date" and subsequent text
            cleaned_str = re.sub(
                r"(D\.?\s*O\s*\.?\s*No|Effec(?:t)?ivity Date)\s*.*", "",
                cleaned_str,
                flags=re.IGNORECASE
            ).strip()

        # Remove "no. <number> -" prefix
        cleaned_str = re.sub(r'^no\.\s*\d+\s*-\s*', '', cleaned_str, flags=re.IGNORECASE).strip()

        # Remove continuation markers like "(cont.)", "continued", etc.
        cleaned_str = re.sub(
            r"\s*-*\s*(\s*\(cont\s*\.?\)|(?:\()?\s*continued\s*(?:\)?)|(?:\()?\s*continuation\s*(?:\))?|(?:\()?\s*continaution\s*(?:\))?)",
            "",
            cleaned_str,
            flags=re.IGNORECASE
        ).strip()

        # Remove "- revised" and subsequent text
        cleaned_str = re.sub(r"\s*-+\s*revised.*", "", cleaned_str, flags=re.IGNORECASE).strip()

        # Remove trailing spaces or underscores
        cleaned_str = re.sub(r'[\s_]+$', '', cleaned_str)

        return cleaned_str


def extract_value(pattern: str, text: str) -> tuple[str | None, bool]:
    """
    Searches for a pattern in text and extracts the first capturing group.

    Args:
        pattern: The regex pattern with one capturing group.
        text: The text to search within.

    Returns:
        A tuple: (extracted_value, True) if found, else (None, False).
    """
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip(), True
    else:
        return None, False


def find_column_headers(df: pd.DataFrame, start_index: int, proximity_window: int = 3, debug: bool = False) -> tuple[
    bool, dict | None, int]:
    """
    Finds column headers for street name, vicinity, classification, and ZV/SQM
    within a DataFrame, starting at `start_index` and looking down `proximity_window` rows.

    Args:
        df: The DataFrame to search.
        start_index: The starting row index in the DataFrame.
        proximity_window: How many rows down to search for headers.
        debug: If True, prints debug information.

    Returns:
        A tuple: (found_all_headers, header_indices_dict, last_row_index_of_headers).
        `header_indices_dict` contains column indices for found headers.
        `last_row_index_of_headers` is the DataFrame index of the last row considered part of the header.
    """
    headers = {
        'street_name_index': None,
        'vicinity_index': None,
        'classification_index': None,
        'zv_sq_m_index': None
    }
    # Stores the offset from start_index where each header was maximally defined
    headers_max_row_offset = {
        'street_name_index': -1,
        'vicinity_index': -1,
        'classification_index': -1,
        'zv_sq_m_index': -1
    }

    current_search_base_index = start_index
    column_texts_combined = {}  # Accumulates text from rows for each column
    extend_search_due_to_keyword = False  # Flag to adjust search window if a keyword is found

    # These holders are for a specific logic for ZV/SQ.M. header detection
    zv_pattern_match_holder = None
    zv_pattern_offset_holder = None

    # This variable is declared in the original code but not used. Kept for fidelity.
    classification_pattern_holder = None

    current_offset = 0
    while current_offset < proximity_window:
        current_df_row_index = current_search_base_index + current_offset
        if current_df_row_index >= len(df):
            break

        row_series = df.iloc[current_df_row_index]

        for col_idx, cell_content in enumerate(row_series):
            cell_str = str(cell_content)
            if col_idx not in column_texts_combined:
                column_texts_combined[col_idx] = cell_str
            else:
                column_texts_combined[col_idx] += ' ' + cell_str

        if debug:
            print(
                f"find_column_headers: Row {current_df_row_index} (offset {current_offset}): Combined texts {column_texts_combined}")

        # Check each column's combined text for header patterns
        for col_idx, combined_text in column_texts_combined.items():
            # Street Name / Subdivision / Condominium
            if headers['street_name_index'] is None:
                street_pattern = (r"(S\s*T\s*R\s*E\s*E\s*T\s*N\s*A\s*M\s*E|"
                                  r"S\s*U\s*B\s*D\s*I\s*V\s*I\s*S\s*I\s*O\s*N|"
                                  r"C\s*O\s*N\s*D\s*O\s*M\s*I\s*N\s*I\s*U\s*M)")
                if re.search(street_pattern, combined_text, re.IGNORECASE):
                    headers['street_name_index'] = col_idx
                    headers_max_row_offset['street_name_index'] = current_df_row_index - start_index
                    if debug: print(
                        f"Street Name header found at col {col_idx}, row_offset {headers_max_row_offset['street_name_index']}")

            # Vicinity
            if headers['vicinity_index'] is None:
                if re.search(r"V\s*I\s*C\s*I\s*N\s*I\s*T\s*Y", combined_text, re.IGNORECASE):
                    headers['vicinity_index'] = col_idx
                    headers_max_row_offset['vicinity_index'] = current_df_row_index - start_index
                    if debug: print(
                        f"Vicinity header found at col {col_idx}, row_offset {headers_max_row_offset['vicinity_index']}")

            # Classification
            if headers['classification_index'] is None:
                class_pattern = (r"CLASS(?:IFICATION)?|"
                                 r"C\s*L\s*A\s*S\s*S\s*I\s*F\s*I\s*C\s*A\s*T\s*I\s*O\s*N")
                if re.search(class_pattern, combined_text, re.IGNORECASE | re.DOTALL):
                    headers['classification_index'] = col_idx
                    headers_max_row_offset['classification_index'] = current_df_row_index - start_index
                    extend_search_due_to_keyword = True
                    if debug: print(
                        f"Classification header found at col {col_idx}, row_offset {headers_max_row_offset['classification_index']}")

            # ZV/SQ.M. (Zonal Value per Square Meter)
            # This logic prefers a ZV header found earlier if multiple matches occur.
            if headers['zv_sq_m_index'] is None or headers[
                'zv_sq_m_index'] < col_idx:  # Check if current column is to the right of a previous ZV find
                zv_pattern = (
                    r"\d+(?:ST|ND|RD|TH)\s+(?:REVISION|Rev)(?:.*Z\.?V\.?.*SQ.*M\.?)?|"
                    r"(?:\d+(?:ST|ND|RD|TH)\s+REVISION|Rev\s+ZV\s+/?.*SQ\.?\s*M\.?)|"
                    r"(?:Z|2)\.?V\.?.*SQ.*M\.?|FINAL"
                )
                match = re.search(zv_pattern, combined_text, re.IGNORECASE)
                if match:
                    headers['zv_sq_m_index'] = col_idx  # Tentatively assign
                    headers_max_row_offset['zv_sq_m_index'] = current_df_row_index - start_index
                    if debug: print(
                        f"ZV/SQ.M. header found at col {col_idx}, row_offset {headers_max_row_offset['zv_sq_m_index']}")

                    if not zv_pattern_match_holder:  # First time ZV pattern is seen
                        zv_pattern_match_holder = match
                        zv_pattern_offset_holder = current_offset
                        headers['zv_sq_m_index'] = None  # Temporarily unset to allow further searching or refinement
                        extend_search_due_to_keyword = True
                    elif zv_pattern_match_holder.group(0) == match.group(
                            0):  # Same pattern as before, potentially on a new line
                        # Revert to the offset of the first ZV pattern sighting.
                        headers_max_row_offset['zv_sq_m_index'] = zv_pattern_offset_holder

        if extend_search_due_to_keyword:
            if debug: print("Extending search window due to keyword match.")
            # This complex adjustment allows re-evaluation of previous rows with more accumulated text
            # or extends the effective proximity window.
            current_offset -= 2
            current_search_base_index += 2
            extend_search_due_to_keyword = False

        current_offset += 1

    # Post-loop adjustments and checks
    # Specific adjustment for classification column if it's between vicinity and ZV with a certain gap
    if headers['zv_sq_m_index'] and headers['vicinity_index']:
        if headers['zv_sq_m_index'] - headers['vicinity_index'] == 4:  # Check if ZV is 4 columns after Vicinity
            if headers['classification_index'] is not None and \
                    headers['classification_index'] - headers[
                'vicinity_index'] == 1:  # And Classification is 1 col after Vicinity
                headers['classification_index'] += 1  # Shift classification one column to the right

    all_headers_found = all(value is not None for value in headers.values())

    if all_headers_found:
        header_column_indices = list(headers.values())
        # Check for duplicate column indices assigned to different headers
        if len(header_column_indices) != len(set(header_column_indices)):
            if debug: print(f"Duplicate header column indices found at base_index {start_index}: {headers}")
            return False, None, start_index  # Indicates an issue

        max_offset_used = max(headers_max_row_offset.values())
        last_header_row_index = start_index + max_offset_used
        if debug:
            print(
                f"All headers found. Max offset used: {max_offset_used}. Last header row index: {last_header_row_index}")
            print(f"Header indices: {headers}")
        return True, headers, last_header_row_index
    else:
        if debug: print(f"Not all headers found starting at base_index {start_index}. Found: {headers}")
        return False, None, start_index


def find_location_components(
        df: pd.DataFrame,
        start_index: int,
        proximity_window: int = 3,
        current_province: str = None,
        current_city: str = None,
        current_barangay: str = None,
        debug: bool = False
) -> tuple[str | None, str | None, str | None, int]:
    """
    Finds Province, City/Municipality, and Barangay information in DataFrame rows.

    Args:
        df: DataFrame to search.
        start_index: Starting row index.
        proximity_window: How many rows down to search.
        current_province, current_city, current_barangay: Existing values (if any).
        debug: If True, print debug info.

    Returns:
        Tuple of (province, city, barangay, last_processed_row_index).
        `last_processed_row_index` is the df index of the last row where a component was found or search ended.
    """
    if debug: print(f"\nRunning find_location_components starting at df index {start_index}")

    # Initialize with existing values or None
    province_val, city_val, barangay_val = current_province, current_city, current_barangay

    last_matched_df_index = start_index  # Tracks the df index of the last relevant row
    initial_search_df_index = start_index  # The original start_index for this call

    # Flags for parsing logic
    expecting_colon_prefixed_values = False  # For "PROVINCE / CITY / ..." combined labels
    any_component_found_this_call = False
    extend_search_window = False  # Similar to find_column_headers, to adjust search scope

    # Holders for specific scenarios where components might appear out of expected order
    # e.g., Barangay found before Province.
    # These indices track the df row where the component was found.
    province_found_at_df_idx, city_found_at_df_idx, barangay_found_at_df_idx = None, None, None
    # These hold the *values* if they are found "too early"
    temp_barangay_holder, temp_city_holder = None, None

    # Stores details like at which df row a component was found (for debugging or complex logic)
    # found_details_log = {} # Original 'found_details' was not fully utilized, simplifying for now

    current_offset = 0
    while current_offset < proximity_window:
        current_df_row_index = start_index + current_offset
        if current_df_row_index >= len(df):
            break

        current_row_series = df.iloc[current_df_row_index]
        # Concatenate non-null stringified cells for pattern matching across the row
        combined_row_text = ''.join(map(str, current_row_series.dropna())).strip()
        non_null_cells_in_row = current_row_series.dropna().astype(str).tolist()

        if debug:
            print(
                f"\nfind_location_components: Processing df_row {current_df_row_index} (offset {current_offset}/{proximity_window - 1})")
            print(f"  Expecting colon-prefixed values: {expecting_colon_prefixed_values}")
            print(f"  Row content (non-null): {non_null_cells_in_row}")

        # Scenario 1: Combined label "PROVINCE / CITY / ..."
        if not expecting_colon_prefixed_values and any(
                re.search(r"PROVINCE\s*/\s*CITY\s*/\s*MUNICIPALITY\s*/\s*BARANGAYS", cell, re.IGNORECASE)
                for cell in non_null_cells_in_row
        ):
            expecting_colon_prefixed_values = True
            # found_details_log['combined_label_found_at_df_row'] = current_df_row_index
            if debug: print(
                f"  Combined location labels found at df_row {current_df_row_index}. Expecting values next.")
            # Don't increment offset here, let the loop do it. The next row should contain values.
            # current_offset += 1 # This was in original, but next `continue` skips the main incrementer.
            # The `continue` moves to the next iteration of the while loop, which will increment offset.
            current_offset += 1
            continue  # Check next row for values

        # Scenario 2: Reading colon-prefixed values after combined label
        if expecting_colon_prefixed_values:
            for cell_text in non_null_cells_in_row:
                cell_text_stripped = cell_text.strip()
                if cell_text_stripped.startswith(":"):
                    value_after_colon = cell_text_stripped.lstrip(":").strip()
                    if not province_val:
                        province_val = clean_value(value_after_colon)
                        # found_details_log['province_found_at_df_row'] = current_df_row_index
                        any_component_found_this_call = True
                        if debug: print(f"  Colon-prefixed Province found: {province_val}")
                    elif not city_val:
                        city_val = clean_value(value_after_colon)
                        # found_details_log['city_found_at_df_row'] = current_df_row_index
                        any_component_found_this_call = True
                        if debug: print(f"  Colon-prefixed City/Municipality found: {city_val}")
                    elif not barangay_val:
                        barangay_val = clean_value(value_after_colon)
                        # found_details_log['barangay_found_at_df_row'] = current_df_row_index
                        any_component_found_this_call = True
                        if debug: print(f"  Colon-prefixed Barangay found: {barangay_val}")

            last_matched_df_index = current_df_row_index
            if all([province_val, city_val, barangay_val]):
                return province_val, city_val, barangay_val, last_matched_df_index + 1  # All found, return

            if current_offset == proximity_window - 1:  # Reached end of window
                # Return whatever was found, or original values if nothing new.
                # If nothing was found, last_matched_df_index is still initial_search_df_index
                return province_val, city_val, barangay_val, initial_search_df_index if not any_component_found_this_call else last_matched_df_index + 1

            current_offset += 1
            continue  # Move to next row within proximity window

        # Scenario 3: Separate labels (Province:, City:, Barangay:)
        else:
            if combined_row_text.lower().startswith("district"):
                if debug: print(f"  Skipping row {current_df_row_index} starting with 'district'.")
                current_offset += 1
                continue

            # Province
            extracted_prov, prov_match = extract_value(r"Province\s*(?::|\s|of)?\s*(.*)", combined_row_text)
            if prov_match:
                province_val = clean_value(extracted_prov)
                any_component_found_this_call = True
                extend_search_window = True
                last_matched_df_index = initial_search_df_index = province_found_at_df_idx = current_df_row_index
                if debug: print(f"  Province label match found: {province_val} at df_row {current_df_row_index}")

            # City/Municipality
            extracted_city, city_match = extract_value(
                r"(?:(?!City,)(?:City|Municipality))(?:\s*\/\s*(?:City|Municipality))?\s*[:\s]?\s*(.+)",
                combined_row_text
            )
            if city_match:
                city_val = clean_value(extracted_city)
                any_component_found_this_call = True
                extend_search_window = True
                last_matched_df_index = initial_search_df_index = city_found_at_df_idx = current_df_row_index
                if debug: print(f"  City/Municipality label match: {city_val} at df_row {current_df_row_index}")

            # Barangay/Zone
            extracted_brgy, brgy_match = extract_value(
                r"(?:Barangays|Zone|Barangay)(?:\s*\/\s*(?:Barangays|Zone|Barangay))?\s*[:\s]?\s*(.+)",
                combined_row_text
            )
            # Avoid misinterpreting "along barangay road" as a Barangay name
            if extracted_brgy and re.search(r".*\s*(?:along\s*)?barangay.*road.*", combined_row_text, re.IGNORECASE):
                if debug: print(
                    f"  Discarding potential Barangay match due to 'along barangay road' pattern: {extracted_brgy}")
                brgy_match = False  # Invalidate match
                extracted_brgy = None

            if brgy_match:
                barangay_val = clean_value(extracted_brgy)
                any_component_found_this_call = True
                extend_search_window = True
                last_matched_df_index = initial_search_df_index = barangay_found_at_df_idx = current_df_row_index
                if debug: print(f"  Barangay/Zone label match: {barangay_val} at df_row {current_df_row_index}")

            if extend_search_window:
                # This logic allows the proximity window to effectively "slide" or "reset" slightly
                # if a component is found, giving more chances to find subsequent components
                # within the conceptual window.
                current_offset -= 1  # Counteract the upcoming increment to re-evaluate from a similar relative offset
                start_index += 1  # Advance the base for the next iteration's current_df_row_index
                extend_search_window = False

            if any_component_found_this_call and all([province_val, city_val, barangay_val]):
                # Specific handling for out-of-order components:
                if barangay_found_at_df_idx is not None and province_found_at_df_idx is not None and \
                        barangay_found_at_df_idx < province_found_at_df_idx and not temp_barangay_holder:
                    if debug: print(
                        "  Barangay found before Province. Holding Barangay and extending search for new Province.")
                    temp_barangay_holder = barangay_val
                    barangay_val = None  # Clear current barangay to find a new one after province
                    current_offset -= 1  # Adjust offset to re-evaluate or give more search iterations
                    start_index += 2  # Adjust base index significantly
                    continue

                if city_found_at_df_idx is not None and province_found_at_df_idx is not None and \
                        city_found_at_df_idx < province_found_at_df_idx and not temp_city_holder:
                    if debug: print("  City found before Province. Holding City and extending search for new Province.")
                    temp_city_holder = city_val
                    city_val = None  # Clear current city
                    current_offset -= 1
                    start_index += 2
                    continue

                if debug: print(f"  All location components found. Last matched df_row: {last_matched_df_index}")
                return province_val, city_val, barangay_val, last_matched_df_index + 1

            if current_offset == proximity_window - 1:  # End of window
                if any_component_found_this_call:
                    # If a holder has a value, it means we were trying to re-find a component. Prioritize it.
                    if temp_barangay_holder and not barangay_val: barangay_val = temp_barangay_holder
                    if temp_city_holder and not city_val: city_val = temp_city_holder
                    return province_val, city_val, barangay_val, last_matched_df_index + 1

                # If nothing found in this call, return original values and the initial start_index
                return current_province, current_city, current_barangay, initial_search_df_index

        current_offset += 1
        if not expecting_colon_prefixed_values and not any_component_found_this_call and current_offset > 0:
            # If not in colon-value mode, and nothing found in the first pass, unlikely to find more.
            # Original code had `break` here. This means if first row of window yields nothing, stop.
            break

            # If loop finishes, return what's gathered.
    # If temp_barangay_holder exists, it implies it should take precedence if barangay_val is still None
    if temp_barangay_holder and not barangay_val: barangay_val = temp_barangay_holder
    if temp_city_holder and not city_val: city_val = temp_city_holder

    # If any component was found, use last_matched_df_index, otherwise return the original start_index (initial_search_df_index)
    final_return_index = last_matched_df_index + 1 if any_component_found_this_call else initial_search_df_index
    return province_val, city_val, barangay_val, final_return_index


def main(
        df: pd.DataFrame,
        debug: bool = False,
        start_row_index: int = 0,
        end_row_index: int = -1,
        debug_location: bool = False,
        debug_header: bool = False
) -> pd.DataFrame:
    """
    Main processing function to extract structured data from the DataFrame.
    It iterates through the DataFrame, identifying location components (province, city, barangay),
    then column headers, and finally extracts data rows into a new structured DataFrame.

    Args:
        df: The input DataFrame from an Excel sheet.
        debug: General debug flag.
        start_row_index: DataFrame row index to start processing from.
        end_row_index: DataFrame row index to end processing at (-1 for end of DataFrame).
        debug_location: Specific debug flag for location component finding.
        debug_header: Specific debug flag for header finding.

    Returns:
        A new pandas DataFrame with structured data.
    """

    # Determine the final row index for processing
    max_row_index = len(df) if end_row_index == -1 else end_row_index
    current_row_index = start_row_index

    processed_table_count = 0
    output_columns = ['Province', 'City/Municipality', 'Barangay',
                      'Street/Subdivision', 'Vicinity', 'Classification', 'ZV/SQM']
    structured_df = pd.DataFrame(columns=output_columns)

    LOCATION_PROXIMITY_WINDOW = 3  # For finding location components
    HEADER_PROXIMITY_WINDOW = 3  # For finding column headers

    # State variables for current location context
    current_province = None
    current_city = None
    current_barangay = None

    # State variable for current table's header column indices
    current_header_indices = None
    is_continuation_table = False  # True if new table is for the same province as previous

    # State variables for carrying over values from previous row within the *same* table
    prev_row_col1_val = None
    prev_row_vicinity_val = None
    # prev_row_classification_val = None # Original code had this, but it's not used. Kept for fidelity.
    # prev_row_zvsqm_val = None        # Original code had this, but it's not used. Kept for fidelity.

    # These holders are for carrying over values for col1 and vicinity if current cell is empty
    # They are reset when a new table starts or when col1/vicinity gets a new non-empty value.
    current_table_col1_holder = None
    current_table_vicinity_holder = None

    # Specific state for "ALL OTHER" street logic
    current_table_all_other_vicinity_cache = None  # Caches the vicinity associated with an "ALL OTHER" street
    prev_row_was_all_other_type = None  # Flag: True if prev_row_col1_val was an "ALL OTHER" type street

    while current_row_index < max_row_index:
        # --- 1. Find Location Components ---
        # Try to find/update location components (Province, City, Barangay)
        # `loc_search_end_idx` is the row index *after* the last row scanned for location info
        new_province, new_city, new_barangay, loc_search_end_idx = find_location_components(
            df, current_row_index,
            proximity_window=LOCATION_PROXIMITY_WINDOW,
            current_province=current_province,  # Pass existing to potentially fill gaps
            current_city=current_city,
            current_barangay=current_barangay,
            debug=debug_location
        )

        # Check if any new location component was actually found or updated
        # This does not mean all are non-None, just that the find function might have changed one.
        location_components_updated = any([
            new_province != current_province and new_province is not None,
            new_city != current_city and new_city is not None,
            new_barangay != current_barangay and new_barangay is not None
        ])

        if debug and (location_components_updated or not all(
                [current_province, current_city, current_barangay])):  # Log if updated or still incomplete
            print(
                f"Location components search ended at df_row {loc_search_end_idx - 1}. Values: Prov='{new_province}', City='{new_city}', Brgy='{new_barangay}'")

        # --- 2. Find Column Headers ---
        # Start searching for headers from where location search left off
        # `header_search_end_idx` is the row index of the *last row of the found header*
        headers_found, new_header_indices, header_search_end_idx = find_column_headers(
            df, loc_search_end_idx,
            proximity_window=HEADER_PROXIMITY_WINDOW,
            debug=debug_header
        )

        if debug and headers_found:
            print(f"Column headers found. Ends at df_row {header_search_end_idx}. Indices: {new_header_indices}")

        # --- 3. Process Table Data or Advance ---
        # Check if we have a valid setup (headers found and at least some location info)
        # The original logic implies that if headers are found, we use the latest location info.
        if headers_found and (
                new_province or new_city or new_barangay or current_province or current_city or current_barangay):
            # A new table or continuation of a table is starting.
            # Update province and determine if it's a continuation
            if new_province and new_province != current_province:
                is_continuation_table = False
                current_province = new_province
            elif new_province == current_province and new_province is not None:  # Explicitly check not None
                is_continuation_table = True
            # else: province remains unchanged or None, continuation status depends on previous state / if it's the first table.

            # Update other location components, prioritizing newly found ones.
            current_city = new_city if new_city else current_city
            current_barangay = new_barangay if new_barangay else current_barangay

            current_header_indices = new_header_indices
            current_row_index = header_search_end_idx + 1  # Start reading data rows *after* the header

            processed_table_count += 1
            if debug:
                print(f'\n{"#" * 20} PROCESSING TABLE {processed_table_count} {"#" * 20}')
                print(f"Location: P={current_province}, C={current_city}, B={current_barangay}")
                print(f"Headers: {current_header_indices}, Is Continuation: {is_continuation_table}\n")

            # Reset states for the new table data
            empty_row_streak = 0
            MAX_EMPTY_ROW_STREAK = 4  # Max consecutive non-data rows before assuming table end

            current_table_col1_holder = None  # Reset for new table
            current_table_vicinity_holder = None  # Reset for new table
            current_table_all_other_vicinity_cache = None  # Reset for new table
            prev_row_was_all_other_type = False  # Reset for new table

            # --- Inner loop: Process data rows for the current table ---
            while current_row_index < max_row_index and empty_row_streak < MAX_EMPTY_ROW_STREAK:
                current_data_row_series = df.iloc[current_row_index]

                # Extract cell values based on found header indices
                # Ensure robust access even if a header index is unexpectedly missing (though find_column_headers should ensure all are present)
                col1_val = current_data_row_series.iloc[
                    current_header_indices['street_name_index']] if current_header_indices.get(
                    'street_name_index') is not None else None
                classification_val = current_data_row_series.iloc[
                    current_header_indices['classification_index']] if current_header_indices.get(
                    'classification_index') is not None else None
                zv_sqm_val = current_data_row_series.iloc[
                    current_header_indices['zv_sq_m_index']] if current_header_indices.get(
                    'zv_sq_m_index') is not None else None

                vicinity_val = None
                vicinity_idx_config = current_header_indices.get('vicinity_index')
                if isinstance(vicinity_idx_config, int):
                    vicinity_val = current_data_row_series.iloc[vicinity_idx_config]
                elif isinstance(vicinity_idx_config, list):  # Handle potential merged vicinity columns
                    vic_str_parts = [str(current_data_row_series.iloc[idx]) for idx in vicinity_idx_config if
                                     idx is not None]
                    vic_str_parts = [s for s in vic_str_parts if s.lower() != 'nan']  # Filter out 'nan' strings
                    vicinity_val = ', '.join(vic_str_parts) if vic_str_parts else None

                if debug:
                    print(
                        f"\n  Raw data at df_row {current_row_index}: Col1='{col1_val}', Vicinity='{vicinity_val}', "
                        f"Class='{classification_val}', ZV='{zv_sqm_val}'")

                # --- Check for new location/header interrupting current table data ---
                # This indicates current table ended and a new one starts immediately
                # First, check if the current data row itself looks like a new location specifier
                temp_prov, temp_city, temp_brgy, temp_loc_idx = find_location_components(df, current_row_index,
                                                                                         proximity_window=1,
                                                                                         debug=False)  # Check only current row

                # Then, check if headers follow immediately after this potential new location
                # header_scan_start_idx = temp_loc_idx # if location found, else current_row_index +1
                # In original, it used new_index_2 which was temp_loc_idx
                is_new_header_present, _, _ = find_column_headers(df, temp_loc_idx, proximity_window=1, debug=False)

                # Combine classification and ZV/SQM to check if they form a valid data point (non-empty after cleaning)
                # This helps decide if the row is data or potentially a new header/location.
                # The original `valid_data_row` logic was more complex. This aims for similar intent:
                # is row clearly data, or could it be metadata for a new table?
                combined_class_zv_str = str(classification_val) + str(zv_sqm_val)
                is_empty_class_zv = not clean_value(combined_class_zv_str)  # True if clean_value is empty string

                if is_empty_class_zv and (any([temp_prov, temp_city, temp_brgy]) and is_new_header_present):
                    if debug:
                        print(
                            f"  New location/header found mid-table at df_row {current_row_index}. Ending current table.")
                    # This row is not data for the current table; it's the start of a new one.
                    # Break from inner data row loop; outer loop will re-evaluate from current_row_index.
                    break  # End current table processing

                # --- Validate data row ---
                # A row is considered invalid if both classification and ZV/SQM are essentially empty.
                # Or if the entire row (when cleaned) is empty.
                cleaned_full_row_str = clean_value(''.join(map(str, current_data_row_series.dropna())).strip())

                # Refined condition for skipping row:
                # If classification and ZV are both null/empty strings
                # AND the cleaned full row string is also empty (original was just `str(cleaned_row).strip()`)
                # OR (original condition) classification is 'nan' string and ZV is not a number.
                is_class_empty = pd.isnull(classification_val) or str(classification_val).strip() == ''
                is_zv_empty = pd.isnull(zv_sqm_val) or str(zv_sqm_val).strip() == ''

                if (is_class_empty and is_zv_empty) or not cleaned_full_row_str:
                    if debug: print(f"  Skipping empty/invalid data row {current_row_index}.")
                    current_row_index += 1
                    empty_row_streak += 1
                    continue

                # Specific skip for "nan" classification if ZV doesn't look like a number
                # (The `replace` is to handle "1.0" as "10" before isdigit)
                if str(classification_val).strip().lower() == 'nan' and not str(zv_sqm_val).replace('.', '',
                                                                                                    1).isdigit():
                    if debug: print(
                        f"  Skipping row {current_row_index} due to 'nan' classification and non-numeric ZV.")
                    current_row_index += 1
                    # empty_row_streak += 1 # Original didn't increment streak here, but seems logical
                    continue

                empty_row_streak = 0  # Valid data found, reset streak

                # --- Handle "ALL OTHER" street logic and carry-over for empty Col1/Vicinity ---
                # If col1_val has a new, non-empty value, reset the all_other_vicinity_cache.
                # This cache should only persist for subsequent empty col1_val rows under an "ALL OTHER" header.
                is_col1_empty_or_nan = pd.isna(col1_val) or not str(col1_val).strip()
                if not is_col1_empty_or_nan:  # col1_val has a new, actual value
                    current_table_all_other_vicinity_cache = None  # Reset cache
                    if debug: print(f"  New Col1 value '{col1_val}' detected. Resetting all_other_vicinity_cache.")

                # Determine if current col1_val is an "ALL OTHER..." type
                current_col1_is_all_other_type = False
                if isinstance(col1_val, str):
                    col1_upper_stripped = col1_val.strip().upper()
                    if col1_upper_stripped.startswith("ALL OTHER") or col1_upper_stripped.startswith("ALL LOTS"):
                        current_col1_is_all_other_type = True

                # Fill col1_val if it's empty
                if is_col1_empty_or_nan:
                    if is_continuation_table:
                        col1_val = current_table_col1_holder if not (pd.isna(current_table_col1_holder) or not str(
                            current_table_col1_holder).strip()) else prev_row_col1_val
                    elif not (pd.isna(current_table_col1_holder) or not str(current_table_col1_holder).strip()):
                        col1_val = current_table_col1_holder
                else:  # col1_val is not empty, so it becomes the new holder for this table
                    current_table_col1_holder = col1_val

                # Fill vicinity_val if it's empty
                is_vicinity_empty_or_nan = pd.isna(vicinity_val) or not str(vicinity_val).strip()
                if is_vicinity_empty_or_nan:
                    if is_continuation_table:
                        # If col1 is new (different from prev_row_col1_val), vicinity_holder should not be used from prev row.
                        # This condition ensures holder is only used if col1 is same or also empty.
                        if (pd.isna(prev_row_col1_val) and pd.isna(col1_val)) or (prev_row_col1_val == col1_val):
                            vicinity_val = current_table_vicinity_holder if not (
                                        pd.isna(current_table_vicinity_holder) or not str(
                                    current_table_vicinity_holder).strip()) else prev_row_vicinity_val
                        # else: vicinity_holder = vicinity_val (which is None/empty) - implicitly done
                    elif not (pd.isna(current_table_vicinity_holder) or not str(current_table_vicinity_holder).strip()):
                        if (pd.isna(prev_row_col1_val) and pd.isna(col1_val)) or (prev_row_col1_val == col1_val):
                            vicinity_val = current_table_vicinity_holder
                        # else: vicinity_holder = vicinity_val
                else:  # vicinity_val is not empty, so it becomes the new holder for this table
                    current_table_vicinity_holder = vicinity_val

                # Update prev_row_was_all_other_type (used in "ALL OTHER" logic below)
                # This flag is set if the *previous data row's col1* was "ALL OTHER", and current col1 is empty.
                if isinstance(prev_row_col1_val, str) and is_col1_empty_or_nan:
                    prev_col1_upper = prev_row_col1_val.strip().upper()
                    prev_row_was_all_other_type = prev_col1_upper.startswith("ALL OTHER") or prev_col1_upper.startswith(
                        "ALL LOTS")
                # else: prev_row_was_all_other_type retains its value (could be False or from an earlier row)
                # It should ideally be reset to False if current col1 is not empty and not "ALL OTHER"
                elif not is_col1_empty_or_nan and not current_col1_is_all_other_type:
                    prev_row_was_all_other_type = False

                # Apply "ALL OTHER" logic to vicinity_val
                if current_col1_is_all_other_type:
                    if not is_vicinity_empty_or_nan:  # If current "ALL OTHER" street has its own vicinity
                        current_table_all_other_vicinity_cache = vicinity_val  # Cache it

                    # If a cache exists (from current or prior "ALL OTHER" row with empty col1)
                    # OR if the previous row was an "ALL OTHER" type (and current col1 is "ALL OTHER" but vicinity is null)
                    if current_table_all_other_vicinity_cache or prev_row_was_all_other_type:
                        vicinity_val = current_table_all_other_vicinity_cache
                    else:  # Current col1 is "ALL OTHER", but its vicinity is null, and no cache/prev_row context
                        vicinity_val = ''
                        if debug: print(
                            f"  'col1' is '{col1_val}'. Setting 'vicinity' to blank as no cache or specific value.")
                else:  # Current col1 is NOT "ALL OTHER" type
                    # If col1 is empty, but logically under a previous "ALL OTHER" street, it might inherit the vicinity.
                    # The current_table_all_other_vicinity_cache was reset if col1 got a new *non-empty* value.
                    # If col1 is *empty* here, cache might still hold a value from a true "ALL OTHER" street higher up.
                    if is_col1_empty_or_nan and current_table_all_other_vicinity_cache:
                        vicinity_val = current_table_all_other_vicinity_cache
                        if debug: print(
                            f"  Empty col1 under 'ALL OTHER' context. Using cached vicinity: {vicinity_val}")
                    # If col1 is not empty AND not "ALL OTHER", then all_other_vicinity_cache was already reset.
                    # The original code had `all_other_vicinity = None` in the else, which means if current col1
                    # is NOT "ALL OTHER", any cached "ALL OTHER" vicinity is wiped. This is now handled by the reset
                    # at the start of the "ALL OTHER" block if col1 has a new non-empty value.
                    # The line `all_other_vicinity = None` from original code in this `else` branch is effectively done
                    # by the reset `if not (pd.isna(col1) or not str(col1).strip()): current_table_all_other_vicinity_cache = None`
                    # unless col1 is empty. If col1 is empty, and not ALL OTHER, it should not clear the cache from a previous ALL OTHER line.

                # Skip rows that are just dashes (e.g., "---", "---", "---", "---")
                def is_dash_str(val):
                    return isinstance(val, str) and re.fullmatch(r"-+", val.strip()) is not None

                if sum(is_dash_str(v) for v in [col1_val, vicinity_val, classification_val, zv_sqm_val]) >= 3:
                    if debug: print(f"  Skipping dashed row {current_row_index}.")
                    current_row_index += 1
                    empty_row_streak += 1  # Counts as an empty row
                    continue

                # --- Append data to structured_df ---
                new_row_data = {
                    'Province': current_province,
                    'City/Municipality': current_city,
                    'Barangay': current_barangay,
                    'Street/Subdivision': clean_value(col1_val, feature=True),
                    'Vicinity': clean_value(vicinity_val, feature=True),
                    'Classification': clean_value(classification_val, feature=True),
                    'ZV/SQM': clean_value(zv_sqm_val, feature=True)  # ZV/SQM is numeric or empty string
                }
                structured_df.loc[len(structured_df)] = new_row_data

                if debug:
                    print(f"  Appended data: {new_row_data}")
                    print("\n  -------")

                # Update previous row values for next iteration's carry-over logic
                prev_row_col1_val = col1_val
                prev_row_vicinity_val = vicinity_val
                # Original code updated these, but they were not used for carry-over logic.
                # prev_row_classification_val = classification_val
                # prev_row_zvsqm_val = zv_sqm_val

                current_row_index += 1
            # --- End of inner data row loop (while current_row_index < max_row_index and empty_row_streak < MAX_EMPTY_ROW_STREAK) ---
            # If loop broke due to empty_row_streak, current_row_index is already at the problematic row.
            # If loop broke due to new_header/location, current_row_index is at that new metadata row.
            # The outer loop will then re-process from this current_row_index.
            continue  # To the next iteration of the outer while loop

        else:  # Headers not found, or critical location info missing
            if debug: print(f"  Skipping row {current_row_index}: No valid headers or insufficient location context.")
            current_row_index += 1  # Advance to next row to continue search

    if debug:
        print(f"\nTotal tables processed: {processed_table_count}")
    return structured_df