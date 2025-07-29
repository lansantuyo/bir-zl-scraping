import os
import re
import pandas as pd
import numpy as np
import csv
import json

# ==============================================================================
# --- CONSTANTS ---
# ==============================================================================

# --- Row Annotation Labels ---
# These constants define the labels used for annotating rows based on their content.
LABEL_LOC_P = "LOC_P"  # Province
LABEL_LOC_C = "LOC_C"  # City / Municipality
LABEL_LOC_B = "LOC_B"  # Barangay
LABEL_LOC_DESCRIPTOR = "LOC_DESCRIPTOR"  # Row with combined location labels like "PROVINCE / CITY / ..."
LABEL_HDR = "HDR"  # Header row
LABEL_DATA = "DATA"  # Data row
LABEL_BLANK = "BLANK"  # Blank row
LABEL_OTHER = "OTHER"  # Other, non-classified row
LABEL_TITLE = "TITLE"  # Placeholder for title rows
LABEL_NOTE = "NOTE"  # Placeholder for note rows

# --- Regex Patterns for Header Identification ---
# These patterns are used to find the column headers in the table.
STREET_HEADER_PATTERN = re.compile(
    r"(S\s*T\s*R\s*E\s*E\s*T\s*N\s*A\s*M\s*E|"
    r"S\s*U\s*B\s*D\s*I\s*V\s*I\s*S\s*I\s*O\s*N|"
    r"C\s*O\s*N\s*D\s*O\s*M\s*I\s*N\s*I\s*U\s*M)",
    re.IGNORECASE
)
VICINITY_HEADER_PATTERN = re.compile(r"V\s*I\s*C\s*I\s*N\s*I\s*T\s*Y", re.IGNORECASE)
CLASSIFICATION_HEADER_PATTERN = re.compile(
    r"CLASS(?:IFICATION)?|"
    r"C\s*L\s*A\s*S\s*S\s*I\s*F\s*I\s*C\s*A\s*T\s*I\s*O\s*N",
    re.IGNORECASE | re.DOTALL
)
ZV_SQM_HEADER_PATTERN = re.compile(
    r"\d+(?:ST|ND|RD|TH)\s+(?:REVISION|Rev)(?:.*Z\.?V\.?.*SQ.*M\.?)?|"
    r"(?:\d+(?:ST|ND|RD|TH)\s+REVISION|Rev\s+ZV\s+/?.*SQ\.?\s*M\.?)|"
    r"(?:Z|2)\.?V\.?.*SQ.*M\.?|FINAL",
    re.IGNORECASE
)

# --- Regex Patterns for Location Identification ---
# These patterns are used to find location details like Province, City, and Barangay.
PROVINCE_PATTERN = re.compile(r"Province\s*(?::|\s|of)?\s*(.*)", re.IGNORECASE)
CITY_PATTERN = re.compile(r"(?:(?!City,)(?:City|Municipality))(?:\s*\/\s*(?:City|Municipality))?\s*[:\s]?\s*(.+)",
                          re.IGNORECASE)
BARANGAY_PATTERN = re.compile(r"(?:Barangays|Zone|Barangay)(?:\s*\/\s*(?:Barangays|Zone|Barangay))?\s*[:\s]?\s*(.+)",
                              re.IGNORECASE)
COMBINED_LOCATION_PATTERN = re.compile(r"PROVINCE\s*/\s*CITY\s*/\s*MUNICIPALITY\s*/\s*BARANGAYS", re.IGNORECASE)

# --- Regex Patterns for Data Cleaning ---
# These patterns are used in the clean_value function to standardize data.
EFFECTIVITY_DATE_PATTERN = re.compile(r"(D\.?\s*O\s*\.?\s*No|Effec(?:t)?ivity Date)\s*.*", re.IGNORECASE)
DO_NO_PATTERN = re.compile(r'^no\.\s*\d+\s*-\s*', re.IGNORECASE)
CONTINUATION_PATTERN = re.compile(
    r"\s*-*\s*(\s*\(cont\s*\.?\)|(?:\()?\s*continued\s*(?:\)?)|(?:\()?\s*continuation\s*(?:\))?|(?:\()?\s*continaution\s*(?:\))?)",
    re.IGNORECASE)
REVISED_PATTERN = re.compile(r"\s*-+\s*revised.*", re.IGNORECASE)


# ==============================================================================
# --- FILE AND DATA HANDLING FUNCTIONS ---
# ==============================================================================

def extract_rdo_number(filename: str) -> int:
    """
    Extracts the RDO number from a filename using a regular expression.

    Args:
        filename: The name of the file.

    Returns:
        The extracted RDO number as an integer, or infinity if not found.
    """
    try:
        match = re.search(r'RDO No\. (\d+)\w? - (.+)\.?(?:xls|xlsx)?', filename, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return float('inf')
    except (ValueError, IndexError) as e:
        print(f"Error processing filename: {filename} - {e}")
        return float('inf')


def xls_to_df(filename: str, base_dir: str = "data/", full_path: str = None) -> tuple:
    """
    Reads a specific Excel sheet into a pandas DataFrame.

    It identifies sheets named 'SheetX', sorts them numerically, and reads the last one.

    Args:
        filename: The Excel filename to convert.
        base_dir: The base directory containing the Excel files.
        full_path: Optional full path to the file, bypassing base_dir.

    Returns:
        A tuple (DataFrame, sheet_name) or (None, None) if an error occurs.
    """
    filepath = full_path if full_path else os.path.join(base_dir, filename)
    try:
        engine = 'xlrd' if filename.lower().endswith('.xls') else 'openpyxl'
        excel_file = pd.ExcelFile(filepath, engine=engine)
        sheet_names = excel_file.sheet_names

        # Find sheets that follow the 'SheetX' naming pattern and sort them.
        target_sheets = sorted(
            [name for name in sheet_names if name.strip().lower().startswith('sheet')],
            key=lambda name: int(re.search(r'\d+', name).group())
        )

        if target_sheets:
            last_sheet_name = target_sheets[-1]
            df = pd.read_excel(filepath, sheet_name=last_sheet_name, header=None)
            return df, last_sheet_name
        else:
            print(f"No matching sheets found in {filename}")
            return None, None
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return None, None


# ==============================================================================
# --- DATA CLEANING AND EXTRACTION HELPERS ---
# ==============================================================================

def clean_value(value, feature: bool = False) -> str or float:
    """
    Cleans and standardizes a given value.

    Args:
        value: The value to clean (can be string, number, etc.).
        feature: A boolean flag to apply a subset of cleaning rules.

    Returns:
        The cleaned value as a float or string.
    """
    # Attempt to convert to a rounded float first.
    try:
        return round(float(value), 3)
    except (ValueError, TypeError):
        value = str(value)
        if value == 'nan':
            return ''
        if value is not None:
            # General cleaning rules
            value = re.sub(r"^\s*:\s*", "", value.strip())
            value = DO_NO_PATTERN.sub('', value).strip()
            value = CONTINUATION_PATTERN.sub("", value).strip()
            value = REVISED_PATTERN.sub("", value).strip()
            value = value.rstrip(' _')

            # Specific rule for non-feature cleaning
            if not feature:
                value = EFFECTIVITY_DATE_PATTERN.sub("", value).strip()
        return value


def extract_value(pattern: re.Pattern, text: str) -> tuple:
    """
    Extracts a value from text using a compiled regex pattern.

    Args:
        pattern: The compiled regular expression to use for matching.
        text: The text to search within.

    Returns:
        A tuple (extracted_value, was_match_found).
    """
    match = pattern.search(text)
    if match:
        return match.group(1).strip(), True
    return None, False


def _update_annotations_cache(cache: dict, index: int, label: str, filename: str, sheetname: str, raw_cells: list):
    """
    Helper to update the annotations cache, prioritizing more specific labels.
    """
    # Define the priority of each label. Higher numbers are higher priority.
    priority = {
        LABEL_LOC_P: 5, LABEL_LOC_C: 5, LABEL_LOC_B: 5, LABEL_LOC_DESCRIPTOR: 4,
        LABEL_HDR: 3, LABEL_DATA: 2, LABEL_BLANK: 1, LABEL_OTHER: 0
    }
    existing_ann = cache.get(index)
    if not existing_ann or priority.get(label, -1) >= priority.get(existing_ann.get("label"), -1):
        cache[index] = {
            "filename": filename,
            "sheetname": sheetname,
            "row_index": index,
            "raw_cells_json": json.dumps(raw_cells),
            "label": label
        }


# ==============================================================================
# --- CORE LOGIC: FINDING LOCATIONS AND HEADERS ---
# ==============================================================================

def find_location_components(df, data, index, annotations_cache, filename_for_ann, sheetname_for_ann,
                             proximity_window=3, current_province=None, current_city=None,
                             current_barangay=None, debug=False):
    """
    Identifies location components (Province, City, Barangay) within a proximity window.
    """
    initial_index = index
    last_matched_index = initial_index
    expecting_values_after_descriptor = False
    found_any = False
    offset = 0

    # Holders for cases where components might be found out of order.
    barangay_holder, city_holder = None, None
    province_index, city_index, barangay_index = None, None, None

    while offset < proximity_window:
        current_index = index + offset
        if current_index >= len(df):
            break

        current_row = data[current_index]
        non_null_cells = [str(val) for val in current_row if not pd.isna(val)]
        combined_current_row = ''.join(non_null_cells).strip()
        raw_cells_list = [str(val) if not pd.isna(val) else "" for val in current_row]
        current_label = LABEL_BLANK if not combined_current_row else LABEL_OTHER

        # --- Case 1: Combined "PROVINCE / CITY / ..." descriptor row ---
        if not expecting_values_after_descriptor and any(
                COMBINED_LOCATION_PATTERN.search(cell) for cell in non_null_cells):
            expecting_values_after_descriptor = True
            current_label = LABEL_LOC_DESCRIPTOR
            if debug: print(f"Combined labels found at row {current_index}")
            _update_annotations_cache(annotations_cache, current_index, current_label, filename_for_ann,
                                      sheetname_for_ann, raw_cells_list)
            offset += 1
            continue

        if expecting_values_after_descriptor:
            for cell in non_null_cells:
                if cell.strip().startswith(":"):
                    value = clean_value(cell.lstrip(":").strip())
                    if not current_province:
                        current_province = value
                        found_any = True
                    elif not current_city:
                        current_city = value
                        found_any = True
                    elif not current_barangay:
                        current_barangay = value
                        found_any = True
            last_matched_index = current_index
            if all([current_province, current_city, current_barangay]) or offset == proximity_window - 1:
                return current_province, current_city, current_barangay, last_matched_index + 1
            offset += 1
            continue

        # --- Case 2: Separate "Province:", "City:", "Barangay:" rows ---
        if combined_current_row.lower().startswith("district"):
            offset += 1
            continue

        province, p_match = extract_value(PROVINCE_PATTERN, combined_current_row)
        city, c_match = extract_value(CITY_PATTERN, combined_current_row)
        barangay, b_match = extract_value(BARANGAY_PATTERN, combined_current_row)

        # Invalidate barangay match if it's part of a descriptive phrase
        if barangay and re.search(r".*\s*(?:along\s*)?barangay.*road.*", combined_current_row, re.IGNORECASE):
            b_match = False

        if p_match:
            current_province = clean_value(province)
            found_any = True
            last_matched_index = initial_index = province_index = current_index
            current_label = LABEL_LOC_P
        if c_match:
            current_city = clean_value(city)
            found_any = True
            last_matched_index = initial_index = city_index = current_index
            current_label = LABEL_LOC_C
        if b_match:
            current_barangay = clean_value(barangay)
            found_any = True
            last_matched_index = initial_index = barangay_index = current_index
            current_label = LABEL_LOC_B

        # This complex logic handles cases where location parts are found out of order.
        # It temporarily holds a value and extends the search if a "higher" part (like province)
        # is expected but a "lower" part (like barangay) was found first.
        if found_any and all([current_province, current_city, current_barangay]):
            if barangay_index is not None and province_index is not None and barangay_index < province_index and not barangay_holder:
                barangay_holder, current_barangay = current_barangay, None
                offset -= 1;
                index += 2;
                continue
            if city_index is not None and province_index is not None and city_index < province_index and not city_holder:
                city_holder, current_city = current_city, None
                offset -= 1;
                index += 2;
                continue
            return current_province, current_city, current_barangay, last_matched_index + 1

        # Final check at the end of the proximity window
        if offset == proximity_window - 1:
            if barangay_holder: current_barangay = barangay_holder
            if city_holder: current_city = city_holder
            if found_any:
                return current_province, current_city, current_barangay, last_matched_index + 1
            return current_province, current_city, current_barangay, initial_index

        _update_annotations_cache(annotations_cache, current_index, current_label, filename_for_ann, sheetname_for_ann,
                                  raw_cells_list)
        offset += 1
        if not expecting_values_after_descriptor and not found_any:
            break

    return current_province, current_city, current_barangay, last_matched_index


def find_column_headers(df, data, index, proximity_window=3, debug=False):
    """
    Finds column headers by accumulating text from rows within a proximity window.
    """
    headers = {'street_name_index': None, 'vicinity_index': None, 'classification_index': None, 'zv_sq_m_index': None}
    headers_max_offset = {'street_name_index': -1, 'vicinity_index': -1, 'classification_index': -1,
                          'zv_sq_m_index': -1}

    original_index = index
    column_texts = {}
    extend_search = False
    offset = 0
    zv_pattern_holder, zv_offset_holder = None, None
    actual_header_df_indices = set()

    while offset < proximity_window:
        current_index = index + offset
        if current_index >= len(df): break

        # Accumulate text from each column across rows.
        for col_idx, cell in enumerate(data[current_index]):
            column_texts.setdefault(col_idx, '')
            column_texts[col_idx] += ' ' + str(cell)

        # Check each column's combined text for header patterns.
        for col_idx, combined_text in column_texts.items():
            if headers['street_name_index'] is None and STREET_HEADER_PATTERN.search(combined_text):
                headers['street_name_index'] = col_idx
                headers_max_offset['street_name_index'] = current_index - original_index
            if headers['vicinity_index'] is None and VICINITY_HEADER_PATTERN.search(combined_text):
                headers['vicinity_index'] = col_idx
                headers_max_offset['vicinity_index'] = current_index - original_index
            if headers['classification_index'] is None and CLASSIFICATION_HEADER_PATTERN.search(combined_text):
                headers['classification_index'] = col_idx
                headers_max_offset['classification_index'] = current_index - original_index
                extend_search = True

            # ZV/SQM logic is complex because it might be split across rows.
            match = ZV_SQM_HEADER_PATTERN.search(combined_text)
            if match and (headers['zv_sq_m_index'] is None or headers['zv_sq_m_index'] < col_idx):
                headers['zv_sq_m_index'] = col_idx
                headers_max_offset['zv_sq_m_index'] = current_index - original_index
                if not zv_pattern_holder:
                    zv_pattern_holder, zv_offset_holder = match, offset
                    headers['zv_sq_m_index'] = None
                    extend_search = True
                elif zv_pattern_holder == match:
                    headers_max_offset['zv_sq_m_index'] = zv_offset_holder

        # If a match was found, note the row index.
        if any(offset_val == (current_index - original_index) for offset_val in headers_max_offset.values()):
            actual_header_df_indices.add(current_index)

        # The search window can be extended if a partial match suggests the header is split.
        if extend_search:
            offset -= 2;
            index += 2
            extend_search = False
        offset += 1

    # Correction for a specific layout where classification is split into 3 columns.
    if headers['zv_sq_m_index'] and headers['vicinity_index']:
        if headers['zv_sq_m_index'] - headers['vicinity_index'] == 4 and \
                headers['classification_index'] - headers['vicinity_index'] == 1:
            headers['classification_index'] += 1

    # Check for success
    if all(value is not None for value in headers.values()):
        # Fail if duplicate column indices were found, as this indicates an error.
        if len(headers.values()) != len(set(headers.values())):
            if debug: print(f"Duplicate header index found at index {index}")
            return False, None, original_index, sorted(list(actual_header_df_indices))

        max_offset_used = max(headers_max_offset.values())
        final_index = original_index + max_offset_used
        return True, headers, final_index, sorted(list(actual_header_df_indices))
    else:
        return False, None, original_index, sorted(list(actual_header_df_indices))


# ==============================================================================
# --- MAIN PROCESSING FUNCTION ---
# ==============================================================================

def mainv3(df, filename_for_ann, sheetname_for_ann, annotations_cache,
           debug=False, start=0, end=-1, debug_location=False, debug_header=False):
    """
    The main function to process a DataFrame, identify tables, and extract data.
    """
    final_index = end if end != -1 else len(df)
    index = start
    data = df.to_numpy()
    output_rows = []

    # Pre-initialize annotations cache for all rows in the processing range.
    for r_idx in range(start, min(final_index, len(df))):
        if r_idx not in annotations_cache:
            raw_cells = [str(val) if not pd.isna(val) else "" for val in df.iloc[r_idx].tolist()]
            label = LABEL_BLANK if not "".join(raw_cells).strip() else LABEL_OTHER
            annotations_cache[r_idx] = {
                "filename": filename_for_ann, "sheetname": sheetname_for_ann, "row_index": r_idx,
                "raw_cells_json": json.dumps(raw_cells), "label": label
            }

    # State variables for tracking across tables
    current_province, current_city, current_barangay = None, None, None
    header_indices = None
    continuation = False
    prev_col1, prev_vicinity, prev_classification, prev_zvsqm = None, None, None, None
    table_count = 0

    while index < final_index:
        # --- Step 1: Find Location and Header Information for a new table ---
        found_components = False
        new_province, new_city, new_barangay, loc_index = find_location_components(
            df, data, index, annotations_cache, filename_for_ann, sheetname_for_ann,
            debug=debug_location
        )
        if any([new_province, new_city, new_barangay]):
            found_components = True

        found_headers, new_headers, new_index, header_df_indices = find_column_headers(
            df, data, loc_index, debug=debug_header
        )

        # Update annotations for identified header rows
        if found_headers:
            for r_idx in header_df_indices:
                if r_idx < len(df):
                    raw_cells = [str(val) if not pd.isna(val) else "" for val in df.iloc[r_idx].tolist()]
                    _update_annotations_cache(annotations_cache, r_idx, LABEL_HDR, filename_for_ann, sheetname_for_ann,
                                              raw_cells)

        # --- Step 2: If a valid table is found, start processing its data rows ---
        if found_headers and found_components:
            table_count += 1
            if debug: print(f'\n--- Processing Table {table_count} ---\n')

            # Update state for the new table
            continuation = (new_province == current_province)
            current_province = new_province if new_province else current_province
            current_city = new_city if new_city else current_city
            current_barangay = new_barangay if new_barangay else current_barangay
            header_indices = new_headers
            index = new_index

            # State variables for the current table's rows
            MAX_BLANK_ROWS = 4  # Max consecutive blank rows before stopping
            blank_row_streak = 0
            col1_holder, vicinity_holder = None, None
            all_other_vicinity = None
            prev_col1_is_all_other = False

            # --- Inner Loop: Process data rows for the current table ---
            while index < final_index and blank_row_streak < MAX_BLANK_ROWS:
                row = data[index]

                # --- Data Row Validation ---
                class_val = row[header_indices['classification_index']]
                zv_val = row[header_indices['zv_sq_m_index']]
                is_row_empty = (pd.isnull(class_val) or str(class_val).strip() == '') and \
                               (pd.isnull(zv_val) or str(zv_val).strip() == '')

                # Check for a new table starting immediately
                _, _, _, next_loc_idx = find_location_components(df, data, index, annotations_cache, filename_for_ann,
                                                                 sheetname_for_ann, proximity_window=1)
                is_new_table, _, _, _ = find_column_headers(df, data, next_loc_idx, proximity_window=1)

                if (is_row_empty and is_new_table):
                    if debug: print(f"New table found at index {index}. Ending current table processing.")
                    break

                if is_row_empty:
                    blank_row_streak += 1
                    index += 1
                    continue
                blank_row_streak = 0  # Reset streak on valid data row

                # --- Extract and Clean Data from Columns ---
                col1 = row[header_indices['street_name_index']]
                vicinity = row[header_indices['vicinity_index']]
                classification = row[header_indices['classification_index']]
                zv = row[header_indices['zv_sq_m_index']]

                # --- Handle Missing 'col1' or 'vicinity' (carry-over logic) ---
                is_col1_null = pd.isna(col1) or not str(col1).strip()
                if is_col1_null:
                    col1 = col1_holder if not (pd.isna(col1_holder) or not str(col1_holder).strip()) else (
                        prev_col1 if continuation else None)
                else:
                    col1_holder = col1
                    all_other_vicinity = None  # Reset on new col1 item

                is_vicinity_null = pd.isna(vicinity) or not str(vicinity).strip()
                if is_vicinity_null:
                    if continuation and prev_col1 == col1:
                        vicinity = vicinity_holder if not (
                                pd.isna(vicinity_holder) or not str(vicinity_holder).strip()) else prev_vicinity
                    elif not (pd.isna(vicinity_holder) or not str(vicinity_holder).strip()):
                        vicinity = vicinity_holder
                else:
                    vicinity_holder = vicinity

                # --- Handle "ALL OTHER" street logic ---
                is_all_other = isinstance(col1, str) and col1.strip().upper().startswith(("ALL OTHER", "ALL LOTS"))
                if is_all_other:
                    if not is_vicinity_null:
                        all_other_vicinity = vicinity  # Capture the vicinity for "ALL OTHER"
                    vicinity = all_other_vicinity or ('' if not prev_col1_is_all_other else None)
                else:
                    all_other_vicinity = None

                prev_col1_is_all_other = isinstance(prev_col1,
                                                    str) and is_col1_null and prev_col1.strip().upper().startswith(
                    ("ALL OTHER", "ALL LOTS"))

                # --- Append Cleaned Data ---
                output_rows.append([
                    current_province, current_city, current_barangay,
                    clean_value(col1, feature=True),
                    clean_value(vicinity, feature=True),
                    clean_value(classification, feature=True),
                    clean_value(zv, feature=True)
                ])

                # Update annotations and previous row state
                raw_cells = [str(val) if not pd.isna(val) else "" for val in row]
                _update_annotations_cache(annotations_cache, index, LABEL_DATA, filename_for_ann, sheetname_for_ann,
                                          raw_cells)
                prev_col1, prev_vicinity, prev_classification, prev_zvsqm = col1, vicinity, classification, zv

                if debug:
                    print(" | ".join(map(str, output_rows[-1])))

                index += 1
            continue  # Proceed to next iteration of the main loop
        else:
            index += 1  # No headers found, advance to the next row

    # --- Step 3: Create Final DataFrame ---
    new_df = pd.DataFrame(output_rows, columns=[
        'Province', 'City/Municipality', 'Barangay',
        'Street/Subdivision', 'Vicinity', 'Classification', 'ZV/SQM'
    ])
    if debug:
        print(f"\nTotal tables processed: {table_count}")
    return new_df
