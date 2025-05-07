import os
import re
import pandas as pd
import csv  # For writing annotations

# --- Constants for Annotation Labels ---
LABEL_LOC_P = "LOC_P"
LABEL_LOC_C = "LOC_C"
LABEL_LOC_B = "LOC_B"
LABEL_HDR = "HDR"
LABEL_DATA = "DATA"
LABEL_BLANK = "BLANK"
LABEL_OTHER = "OTHER"
LABEL_TITLE = "TITLE"  # If you have logic to detect titles, not present in current main
LABEL_NOTE = "NOTE"  # If you have logic to detect notes, not present in current main


def extract_rdo_number(filename: str) -> float:
    # ... (your existing code)
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
    # ... (your existing code)
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
    # ... (your existing code)
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
    # ... (your existing code)
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip(), True
    else:
        return None, False


def find_column_headers(df: pd.DataFrame, start_index: int, proximity_window: int = 3, debug: bool = False) -> tuple[
    bool, dict | None, int]:
    # ... (your existing code)
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
        debug: bool = False,
        # --- New parameter for annotation ---
        annotations_list: list = None,
        filename_for_ann: str = "unknown_file",
        sheetname_for_ann: str = "unknown_sheet"
) -> tuple[str | None, str | None, str | None, int]:
    # ... (rest of the function signature is the same)
    if debug: print(f"\nRunning find_location_components starting at df index {start_index}")

    province_val, city_val, barangay_val = current_province, current_city, current_barangay
    last_matched_df_index = start_index
    initial_search_df_index = start_index
    expecting_colon_prefixed_values = False
    any_component_found_this_call = False
    extend_search_window = False
    province_found_at_df_idx, city_found_at_df_idx, barangay_found_at_df_idx = None, None, None
    temp_barangay_holder, temp_city_holder = None, None

    # --- Annotation: Keep track of rows processed by this function call ---
    # And their tentative labels before a final decision is made by this function
    # For location, it's tricky because a row might contain a label, but the value extraction fails,
    # or it might be a combined label row. We'll try to label the rows where a component *value* is set.

    rows_processed_in_this_call = []  # Store (df_row_index, combined_row_text)

    current_offset = 0
    while current_offset < proximity_window:
        current_df_row_index = start_index + current_offset
        if current_df_row_index >= len(df):
            break

        current_row_series = df.iloc[current_df_row_index]
        combined_row_text = ' '.join(map(str, current_row_series.dropna())).strip()  # Use space for better readability
        non_null_cells_in_row = current_row_series.dropna().astype(str).tolist()

        rows_processed_in_this_call.append(
            {'index': current_df_row_index, 'text': combined_row_text, 'label': LABEL_OTHER})

        if debug:
            print(
                f"\nfind_location_components: Processing df_row {current_df_row_index} (offset {current_offset}/{proximity_window - 1})")
            # ... (rest of debug prints)

        # --- Logic for identifying location components ---
        # (Your existing logic here)
        # --- WHEN A COMPONENT IS IDENTIFIED AND ITS VALUE SET, UPDATE ITS LABEL ---
        # Example modification for Province:
        # Province
        original_province_val = province_val  # Store before attempting to update
        extracted_prov, prov_match = extract_value(r"Province\s*(?::|\s|of)?\s*(.*)", combined_row_text)
        if prov_match:
            province_val = clean_value(extracted_prov)
            if province_val != original_province_val and province_val:  # If value actually changed and is not empty
                # Mark this row as LOC_P in our temporary list for this call
                for row_info in rows_processed_in_this_call:
                    if row_info['index'] == current_df_row_index:
                        row_info['label'] = LABEL_LOC_P
                        break
            any_component_found_this_call = True  # Original logic
            extend_search_window = True  # Original logic
            last_matched_df_index = initial_search_df_index = province_found_at_df_idx = current_df_row_index  # Original logic
            if debug: print(f"  Province label match found: {province_val} at df_row {current_df_row_index}")

        # Similar modifications for City and Barangay:
        # City/Municipality
        original_city_val = city_val
        extracted_city, city_match = extract_value(
            r"(?:(?!City,)(?:City|Municipality))(?:\s*\/\s*(?:City|Municipality))?\s*[:\s]?\s*(.+)",
            combined_row_text
        )
        if city_match:
            city_val = clean_value(extracted_city)
            if city_val != original_city_val and city_val:
                for row_info in rows_processed_in_this_call:
                    if row_info['index'] == current_df_row_index:
                        row_info['label'] = LABEL_LOC_C
                        break
            any_component_found_this_call = True
            extend_search_window = True
            last_matched_df_index = initial_search_df_index = city_found_at_df_idx = current_df_row_index
            if debug: print(f"  City/Municipality label match: {city_val} at df_row {current_df_row_index}")

        # Barangay/Zone
        original_barangay_val = barangay_val
        extracted_brgy, brgy_match = extract_value(
            r"(?:Barangays|Zone|Barangay)(?:\s*\/\s*(?:Barangays|Zone|Barangay))?\s*[:\s]?\s*(.+)",
            combined_row_text
        )
        if extracted_brgy and re.search(r".*\s*(?:along\s*)?barangay.*road.*", combined_row_text, re.IGNORECASE):
            brgy_match = False
            extracted_brgy = None
        if brgy_match:
            barangay_val = clean_value(extracted_brgy)
            if barangay_val != original_barangay_val and barangay_val:
                for row_info in rows_processed_in_this_call:
                    if row_info['index'] == current_df_row_index:
                        row_info['label'] = LABEL_LOC_B
                        break
            any_component_found_this_call = True
            extend_search_window = True
            last_matched_df_index = initial_search_df_index = barangay_found_at_df_idx = current_df_row_index
            if debug: print(f"  Barangay/Zone label match: {barangay_val} at df_row {current_df_row_index}")

        # Combined label logic
        if not expecting_colon_prefixed_values and any(
                re.search(r"PROVINCE\s*/\s*CITY\s*/\s*MUNICIPALITY\s*/\s*BARANGAYS", cell, re.IGNORECASE)
                for cell in non_null_cells_in_row
        ):
            expecting_colon_prefixed_values = True
            for row_info in rows_processed_in_this_call:  # Mark the label row itself
                if row_info['index'] == current_df_row_index:
                    row_info['label'] = LABEL_TITLE  # Or a new "LOC_COMBINED_LABEL"
                    break
            current_offset += 1
            continue

        if expecting_colon_prefixed_values:
            # If we are in this mode, the values are usually on the *next* row,
            # and cells start with ':'. The current row was the label.
            # The logic below is for when value is extracted from a cell with ':'
            # This needs careful alignment with how your existing function sets province_val etc.
            # The key is to tag the row from which the value was *actually set*.
            colon_values_found_on_this_row = False
            for cell_text in non_null_cells_in_row:
                cell_text_stripped = cell_text.strip()
                if cell_text_stripped.startswith(":"):
                    value_after_colon = cell_text_stripped.lstrip(":").strip()
                    cleaned_colon_val = clean_value(value_after_colon)
                    if cleaned_colon_val:  # Only if we got a meaningful value
                        colon_values_found_on_this_row = True
                        if not province_val:  # Assuming they appear in P, C, B order
                            province_val = cleaned_colon_val
                            for row_info in rows_processed_in_this_call:
                                if row_info['index'] == current_df_row_index: row_info['label'] = LABEL_LOC_P
                        elif not city_val:
                            city_val = cleaned_colon_val
                            for row_info in rows_processed_in_this_call:
                                if row_info['index'] == current_df_row_index: row_info['label'] = LABEL_LOC_C
                        elif not barangay_val:
                            barangay_val = cleaned_colon_val
                            for row_info in rows_processed_in_this_call:
                                if row_info['index'] == current_df_row_index: row_info['label'] = LABEL_LOC_B
            if colon_values_found_on_this_row:
                any_component_found_this_call = True
                last_matched_df_index = current_df_row_index

            # ... (rest of your existing logic) ...
            if all([province_val, city_val, barangay_val]):
                # --- Annotation finalization for this call ---
                if annotations_list is not None:
                    for row_info in rows_processed_in_this_call:
                        annotations_list.append({
                            "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                            "row_index": row_info['index'], "raw_text": row_info['text'], "label": row_info['label']
                        })
                return province_val, city_val, barangay_val, last_matched_df_index + 1
            # ...

        # ... (Rest of your find_location_components logic) ...
        # Ensure extend_search_window, current_offset increments are handled.

        current_offset += 1  # Ensure this is correctly placed relative to your existing control flow
        # ... (rest of find_location_components, especially the return conditions)

    # --- Annotation finalization for this call (if loop finishes or breaks early) ---
    if annotations_list is not None:
        for row_info in rows_processed_in_this_call:
            # If a row was processed but didn't get a specific LOC label, it remains OTHER or BLANK if truly empty
            if not row_info['text'].strip() and row_info['label'] == LABEL_OTHER:  # Check if it was empty
                row_info['label'] = LABEL_BLANK

            annotations_list.append({
                "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                "row_index": row_info['index'], "raw_text": row_info['text'], "label": row_info['label']
            })

    final_return_index = last_matched_df_index + 1 if any_component_found_this_call else initial_search_df_index
    return province_val, city_val, barangay_val, final_return_index


def main(
        df: pd.DataFrame,
        filename_for_ann: str,  # For annotation context
        sheetname_for_ann: str,  # For annotation context
        annotations_list: list,  # Pass the global list here
        debug: bool = False,
        start_row_index: int = 0,
        end_row_index: int = -1,
        debug_location: bool = False,
        debug_header: bool = False
) -> pd.DataFrame:
    max_row_index = len(df) if end_row_index == -1 else min(end_row_index,
                                                            len(df))  # Ensure end_row_index is not out of bounds
    current_row_index = start_row_index

    processed_table_count = 0
    output_columns = ['Province', 'City/Municipality', 'Barangay',
                      'Street/Subdivision', 'Vicinity', 'Classification', 'ZV/SQM']
    structured_df = pd.DataFrame(columns=output_columns)

    LOCATION_PROXIMITY_WINDOW = 3
    HEADER_PROXIMITY_WINDOW = 3

    current_province = None
    current_city = None
    current_barangay = None
    current_header_indices = None
    is_continuation_table = False
    prev_row_col1_val = None
    prev_row_vicinity_val = None
    current_table_col1_holder = None
    current_table_vicinity_holder = None
    current_table_all_other_vicinity_cache = None
    prev_row_was_all_other_type = None

    # --- Annotation: Track row indices that have already been explicitly labeled by a function call ---
    # This helps avoid re-labeling rows that were part of a previous component search.
    # It's a bit tricky because find_location_components itself returns the *next* index to process.
    # We will label rows based on the *block* they belong to.

    # To simplify, we'll create a temporary list of labels for all rows in the df, initialized to OTHER
    # This will be populated as the main loop progresses.
    # This is an alternative to passing annotations_list into helper functions and merging.
    # However, your request was to integrate into find_location_components, so we'll stick to that,
    # but be mindful that helper functions need to append to the *global* annotations_list.
    # A dict to quickly check if a row has been annotated by a specific stage
    # to avoid overwriting a more specific label with a generic one later.
    annotated_row_indices_map = {}  # {row_idx: label}

    # --- Initial pass to label all rows as OTHER or BLANK ---
    # This ensures every row gets at least a base label.
    # This part is removed because find_location_components will append if passed the list.
    # The main loop will handle rows not touched by find_location or find_header.

    processed_rows_in_main_loop = set()  # Track rows processed by main logic for DATA/BLANK/OTHER

    while current_row_index < max_row_index:
        # --- For rows skipped before location search ---
        # This part is tricky. find_location_components will annotate the rows it scans.
        # If main loop advances current_row_index directly, those rows need labeling.

        # Store the starting index for this iteration's component search
        iteration_start_index = current_row_index

        # --- 1. Find Location Components ---
        # `loc_search_end_idx` is the row index *after* the last row scanned for location info
        # We pass the global annotations_list to be appended to by find_location_components
        # For rows processed by find_location_components, they are added to annotations_list inside it.
        # The rows_processed_in_this_call in find_location_components should be added to the global list.

        # We need to know which rows find_location_components *actually* scanned.
        # Let's assume find_location_components itself will add to annotations_list for the rows it processes.

        # Call find_location_components
        # Note: `current_province` etc. are passed to retain state across calls
        _start_loc_scan = current_row_index
        temp_prov, temp_city, temp_brgy, loc_search_end_idx = find_location_components(
            df, current_row_index,
            proximity_window=LOCATION_PROXIMITY_WINDOW,
            current_province=current_province,
            current_city=current_city,
            current_barangay=current_barangay,
            debug=debug_location,
            annotations_list=annotations_list,  # Pass the list here
            filename_for_ann=filename_for_ann,
            sheetname_for_ann=sheetname_for_ann
        )
        # Rows from _start_loc_scan up to loc_search_end_idx-1 have been annotated by find_location_components
        for r_idx in range(_start_loc_scan, loc_search_end_idx):
            processed_rows_in_main_loop.add(r_idx)
            # Their labels are already in annotations_list via the helper

        # Update current location based on findings
        # This logic determines if a *new definitive* location component was found to update the state
        if temp_prov and temp_prov != current_province: current_province = temp_prov
        if temp_city and temp_city != current_city: current_city = temp_city
        if temp_brgy and temp_brgy != current_barangay: current_barangay = temp_brgy

        # --- 2. Find Column Headers ---
        _start_hdr_scan = loc_search_end_idx
        headers_found, new_header_indices, header_search_end_idx = find_column_headers(
            df, loc_search_end_idx,  # Start search after location block
            proximity_window=HEADER_PROXIMITY_WINDOW,
            debug=debug_header
        )

        # Annotate header rows
        if headers_found:
            for r_idx in range(_start_hdr_scan, header_search_end_idx + 1):
                if r_idx < max_row_index and r_idx not in processed_rows_in_main_loop:  # Avoid re-annotating if somehow overlapped
                    row_text_hdr = ' '.join(map(str, df.iloc[r_idx].dropna())).strip()
                    annotations_list.append({
                        "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                        "row_index": r_idx, "raw_text": row_text_hdr, "label": LABEL_HDR
                    })
                    processed_rows_in_main_loop.add(r_idx)
        else:  # No headers found, rows scanned by find_column_headers are OTHER or BLANK
            # The range scanned by find_column_headers is loc_search_end_idx to loc_search_end_idx + proximity - 1
            # header_search_end_idx in this case is the start_index it was called with.
            # More accurately, it scanned up to loc_search_end_idx + actual_offsets_tried.
            # For simplicity, if headers_found is false, header_search_end_idx is the input start_index.
            # The rows it *tried* to find headers in, but failed.
            # find_column_headers doesn't return the *actual* last row it scanned if it fails, only the input start_index.
            # This part is a bit tricky to auto-label perfectly without more info from find_column_headers.
            # For now, we'll assume rows between loc_search_end_idx and where data processing starts (or next loc search)
            # will be caught later as OTHER/BLANK if not DATA.
            pass

        # --- 3. Process Table Data or Advance ---
        if headers_found and (
                current_province or current_city or current_barangay or new_header_indices):  # Added new_header_indices
            # A table is identified
            # Update province, city, barangay if new ones were confirmed by find_location_components
            # The current_province, current_city, current_barangay are already updated from temp_ values

            current_header_indices = new_header_indices
            data_processing_start_index = header_search_end_idx + 1

            # Before starting data processing, label rows between header and data_processing_start_index
            # (if any, usually none) as BLANK/OTHER
            for r_idx in range(loc_search_end_idx, data_processing_start_index):  # Covers header rows too
                if r_idx < max_row_index and r_idx not in processed_rows_in_main_loop:
                    row_text_inter = ' '.join(map(str, df.iloc[r_idx].dropna())).strip()
                    label = LABEL_BLANK if not row_text_inter else LABEL_OTHER
                    if r_idx >= _start_hdr_scan and r_idx <= header_search_end_idx and headers_found:  # If it's a header row
                        label = LABEL_HDR
                    annotations_list.append({
                        "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                        "row_index": r_idx, "raw_text": row_text_inter, "label": label
                    })
                    processed_rows_in_main_loop.add(r_idx)

            current_row_index = data_processing_start_index  # Actual data rows start here

            processed_table_count += 1
            # ... (Reset states for new table data as before) ...
            empty_row_streak = 0
            MAX_EMPTY_ROW_STREAK = 4

            # --- Inner loop: Process data rows ---
            _start_data_scan = current_row_index
            _end_data_scan = _start_data_scan  # Will be updated

            while current_row_index < max_row_index and empty_row_streak < MAX_EMPTY_ROW_STREAK:
                _end_data_scan = current_row_index  # Track the last row attempted in this data block
                current_data_row_series = df.iloc[current_row_index]
                row_text_data = ' '.join(map(str, current_data_row_series.dropna())).strip()

                # ... (Your existing data extraction logic: col1_val, vicinity_val, etc.) ...
                col1_val = current_data_row_series.iloc[
                    current_header_indices['street_name_index']] if current_header_indices.get(
                    'street_name_index') is not None and current_header_indices['street_name_index'] < len(
                    current_data_row_series) else None
                classification_val = current_data_row_series.iloc[
                    current_header_indices['classification_index']] if current_header_indices.get(
                    'classification_index') is not None and current_header_indices['classification_index'] < len(
                    current_data_row_series) else None
                zv_sqm_val = current_data_row_series.iloc[
                    current_header_indices['zv_sq_m_index']] if current_header_indices.get(
                    'zv_sq_m_index') is not None and current_header_indices['zv_sq_m_index'] < len(
                    current_data_row_series) else None
                # ... (vicinity_val logic) ...
                vicinity_val = None
                vicinity_idx_config = current_header_indices.get('vicinity_index')
                if isinstance(vicinity_idx_config, int) and vicinity_idx_config < len(current_data_row_series):
                    vicinity_val = current_data_row_series.iloc[vicinity_idx_config]
                # ...

                # --- Check for new location/header interrupting data ---
                # (Your existing logic for this check)
                temp_prov_check, temp_city_check, temp_brgy_check, temp_loc_idx_check = find_location_components(df,
                                                                                                                 current_row_index,
                                                                                                                 proximity_window=1,
                                                                                                                 debug=False)
                is_new_header_present_check, _, _ = find_column_headers(df, temp_loc_idx_check, proximity_window=1,
                                                                        debug=False)
                combined_class_zv_str = str(classification_val) + str(zv_sqm_val)
                is_empty_class_zv = not clean_value(combined_class_zv_str)

                if is_empty_class_zv and (
                        any([temp_prov_check, temp_city_check, temp_brgy_check]) and is_new_header_present_check):
                    # This row is start of new table, so it's not DATA for current table.
                    # It will be labeled by the next iteration's find_location_components or find_column_headers.
                    # So, we break and current_row_index remains, to be re-processed.
                    if current_row_index not in processed_rows_in_main_loop:  # Should be labeled by next cycle
                        annotations_list.append({  # Tentatively label as OTHER, will be refined
                            "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                            "row_index": current_row_index, "raw_text": row_text_data, "label": LABEL_OTHER
                        })
                        processed_rows_in_main_loop.add(current_row_index)
                    break  # End current table processing

                # --- Validate data row & Skip ---
                # (Your existing validation logic)
                is_class_empty = pd.isnull(classification_val) or str(classification_val).strip() == ''
                is_zv_empty = pd.isnull(zv_sqm_val) or str(zv_sqm_val).strip() == ''
                cleaned_full_row_str = clean_value(''.join(map(str, current_data_row_series.dropna())).strip())

                is_actually_empty_looking = not row_text_data.strip()  # Check if original row text is empty

                if (
                        is_class_empty and is_zv_empty and not cleaned_full_row_str) or is_actually_empty_looking:  # Added check for truly empty
                    # This row is considered BLANK for annotation
                    if current_row_index not in processed_rows_in_main_loop:
                        annotations_list.append({
                            "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                            "row_index": current_row_index, "raw_text": row_text_data, "label": LABEL_BLANK
                        })
                        processed_rows_in_main_loop.add(current_row_index)
                    current_row_index += 1
                    empty_row_streak += 1
                    continue

                # Specific skip for "nan" classification
                if str(classification_val).strip().lower() == 'nan' and not str(zv_sqm_val).replace('.', '',
                                                                                                    1).isdigit():
                    if current_row_index not in processed_rows_in_main_loop:  # Skipped row, so OTHER or BLANK
                        annotations_list.append({
                            "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                            "row_index": current_row_index, "raw_text": row_text_data,
                            "label": LABEL_BLANK if is_actually_empty_looking else LABEL_OTHER
                        })
                        processed_rows_in_main_loop.add(current_row_index)
                    current_row_index += 1
                    continue

                # Check for dashed rows (example)
                def is_dash_str(val):
                    return isinstance(val, str) and re.fullmatch(r"-+", val.strip()) is not None

                if sum(is_dash_str(v) for v in [col1_val, vicinity_val, classification_val, zv_sqm_val]) >= 3:
                    if current_row_index not in processed_rows_in_main_loop:  # Dashed row treated as BLANK
                        annotations_list.append({
                            "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                            "row_index": current_row_index, "raw_text": row_text_data, "label": LABEL_BLANK
                        })
                        processed_rows_in_main_loop.add(current_row_index)
                    current_row_index += 1
                    empty_row_streak += 1
                    continue

                empty_row_streak = 0  # Valid data found

                # --- Append data to structured_df ---
                # (Your existing appending logic)
                # --- ANNOTATE AS DATA ROW ---
                if current_row_index not in processed_rows_in_main_loop:
                    annotations_list.append({
                        "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                        "row_index": current_row_index, "raw_text": row_text_data, "label": LABEL_DATA
                    })
                    processed_rows_in_main_loop.add(current_row_index)

                new_row_data = {  # ... your new_row_data ...
                    'Province': current_province, 'City/Municipality': current_city, 'Barangay': current_barangay,
                    'Street/Subdivision': clean_value(col1_val, feature=True),
                    'Vicinity': clean_value(vicinity_val, feature=True),
                    'Classification': clean_value(classification_val, feature=True),
                    'ZV/SQM': clean_value(zv_sqm_val, feature=True)
                }
                structured_df.loc[len(structured_df)] = new_row_data
                # ... (Update prev_row values) ...
                current_row_index += 1
            # --- End of inner data row loop ---
            # Rows from _start_data_scan to _end_data_scan (inclusive) were processed or skipped by data loop
            # and should have been added to annotations_list.
            continue  # To the next iteration of the outer while loop for next table

        else:  # Headers not found, or critical location info missing after trying to find location/headers
            # Rows from iteration_start_index up to current_row_index (or loc_search_end_idx if that's further)
            # were scanned but didn't form a table. Label them OTHER or BLANK.
            # loc_search_end_idx is where the component search stopped.
            # header_search_end_idx is where header search stopped (or started if no headers found).
            # The actual current_row_index for the next iteration will be loc_search_end_idx or
            # header_search_end_idx + 1 if headers were found, or simply current_row_index + 1.

            # The rows between iteration_start_index and loc_search_end_idx (exclusive for loc_search_end_idx)
            # were already handled by find_location_components.
            # Rows between loc_search_end_idx and (if headers_found==False) some point...
            # This part is tricky. If no table is formed, current_row_index will just increment.
            # Any row not covered by find_location or find_header or data loop needs default labeling.

            # If we reach here, it means the block from `iteration_start_index` up to `loc_search_end_idx`
            # (or further if `find_column_headers` scanned more) did not result in a table.
            # `find_location_components` should have annotated its scanned rows.
            # `find_column_headers` annotated its found header rows.
            # If `headers_found` is false, the rows scanned by `find_column_headers` are effectively "other".

            # The rows that find_column_headers *scanned* but found nothing in need labeling
            # This is approximately loc_search_end_idx to loc_search_end_idx + HEADER_PROXIMITY_WINDOW -1
            # if headers_found is false.
            if not headers_found:
                # header_search_end_idx is the start_index it was called with if it fails.
                # So it scanned from header_search_end_idx up to header_search_end_idx + some_offset_it_tried < HEADER_PROXIMITY_WINDOW
                # We'll label the single row at header_search_end_idx (which is loc_search_end_idx here)
                # if it hasn't been processed yet.
                idx_to_label_if_no_table = loc_search_end_idx  # This is where header search started
                if idx_to_label_if_no_table < max_row_index and idx_to_label_if_no_table not in processed_rows_in_main_loop:
                    row_text_skipped = ' '.join(map(str, df.iloc[idx_to_label_if_no_table].dropna())).strip()
                    annotations_list.append({
                        "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                        "row_index": idx_to_label_if_no_table, "raw_text": row_text_skipped,
                        "label": LABEL_BLANK if not row_text_skipped else LABEL_OTHER
                    })
                    processed_rows_in_main_loop.add(idx_to_label_if_no_table)

            current_row_index = max(current_row_index + 1, loc_search_end_idx)  # Ensure progress
            # if headers_found was false, header_search_end_idx was just loc_search_end_idx.

    # --- Final pass for any rows not explicitly processed by the main loop sections ---
    # (e.g. rows at the very end of the sheet after the last table, or rows skipped by large jumps)
    all_df_indices = set(range(start_row_index, max_row_index))
    unprocessed_indices = sorted(list(all_df_indices - processed_rows_in_main_loop))

    for r_idx in unprocessed_indices:
        if r_idx < len(df):  # Check bounds again
            row_text_unprocessed = ' '.join(map(str, df.iloc[r_idx].dropna())).strip()
            annotations_list.append({
                "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                "row_index": r_idx, "raw_text": row_text_unprocessed,
                "label": LABEL_BLANK if not row_text_unprocessed else LABEL_OTHER
            })
            processed_rows_in_main_loop.add(r_idx)  # Mark as processed for sanity

    if debug:
        print(f"\nTotal tables processed: {processed_table_count}")
    return structured_df


# --- Main execution block (example) ---
if __name__ == "__main__":
    # --- Example Setup ---
    data_dir = "your_excel_files_directory/"  # <--- IMPORTANT: SET THIS
    output_annotations_csv = "pseudo_annotations.csv"
    all_annotations_for_csv = []  # This will hold all annotations from all files

    # Create dummy excel file for testing if it doesn't exist
    dummy_file_path = os.path.join(data_dir, "RDO No. 1 - Test RDO.xlsx")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    if not os.path.exists(dummy_file_path):
        print(f"Creating dummy file: {dummy_file_path}")
        dummy_data = {
            0: ["Test File Title", None, None, None],
            1: ["Province:", "Metro Manila", None, None],
            2: ["City/Municipality:", "Test City", None, None],
            3: [None, None, None, None],
            4: ["STREET", "VICINITY", "CLASS", "ZV/SQM"],
            5: ["Main St", "Near Park", "CR", 5000],
            6: ["Side St", "Near Mall", "RR", 3000],
            7: [None, None, None, None],
            8: ["Another Province:", "Test Province 2", None, None]
        }
        dummy_df_excel = pd.DataFrame(dummy_data).T  # Transpose to get rows as dict keys
        try:
            dummy_df_excel.to_excel(dummy_file_path, sheet_name="Sheet1", index=False, header=False)
            print(f"Dummy file {dummy_file_path} created successfully.")
        except Exception as e:
            print(f"Error creating dummy file: {e}")
            # If dummy creation fails, the script might not run correctly without actual files.
            # Consider adding a check here or exiting if no files are found.

    files_to_process = [f for f in os.listdir(data_dir) if f.lower().endswith(('.xls', '.xlsx'))]
    if not files_to_process:
        print(f"No Excel files found in {data_dir}. Please add some files or check the path.")
        # exit() # Optional: exit if no files

    for filename in files_to_process:
        print(f"\nProcessing file: {filename}")
        df_excel, sheet_name = xls_to_df(filename, base_dir=data_dir)

        if df_excel is not None and sheet_name is not None:
            # Reset annotations list for each file if you want separate annotation files per excel
            # Or use a global list like all_annotations_for_csv for one big file
            # For this example, we use the global all_annotations_for_csv

            print(f"Successfully read sheet '{sheet_name}' from {filename}")
            structured_data = main(
                df_excel,
                filename_for_ann=filename,
                sheetname_for_ann=sheet_name,
                annotations_list=all_annotations_for_csv,
                debug=True,  # Enable debug prints
                debug_location=True,
                debug_header=True
            )
            print(f"\n--- Structured Data for {filename} ---")
            print(structured_data.head())
            print("...")
        else:
            print(f"Could not process DataFrame from {filename}")

    # --- Write all collected annotations to a single CSV file ---
    if all_annotations_for_csv:
        # Deduplicate annotations (important if rows could be added multiple times by different logic paths)
        # A simple way is to convert to list of tuples and then to set and back, based on unique (file, sheet, row_index)
        seen_annotations = set()
        final_unique_annotations = []
        for ann in all_annotations_for_csv:
            # Create a unique key for each annotation entry
            # Using only row_index for uniqueness *within a sheet*
            # For global uniqueness, use (filename, sheetname, row_index)
            ann_key = (ann["filename"], ann["sheetname"], ann["row_index"])
            if ann_key not in seen_annotations:
                final_unique_annotations.append(ann)
                seen_annotations.add(ann_key)
            else:  # If seen, we might want to update if the new label is more specific, e.g. OTHER -> DATA
                # This requires more complex logic, for now, first one wins or last one based on order.
                # Let's make it so that more specific labels (not OTHER/BLANK) can overwrite.
                # Find existing and update if new is better
                for i, existing_ann in enumerate(final_unique_annotations):
                    if (existing_ann["filename"], existing_ann["sheetname"], existing_ann["row_index"]) == ann_key:
                        # Prioritize more specific labels over generic ones
                        priority = {LABEL_LOC_P: 5, LABEL_LOC_C: 5, LABEL_LOC_B: 5, LABEL_HDR: 4, LABEL_DATA: 3,
                                    LABEL_TITLE: 2, LABEL_NOTE: 2, LABEL_BLANK: 1, LABEL_OTHER: 0}
                        if priority.get(ann["label"], -1) > priority.get(existing_ann["label"], -1):
                            final_unique_annotations[i] = ann  # Update with more specific label
                        break

        # Sort by filename, sheetname, then row_index for consistent output
        final_unique_annotations.sort(key=lambda x: (x["filename"], x["sheetname"], x["row_index"]))

        print(f"\nWriting {len(final_unique_annotations)} pseudo-annotations to {output_annotations_csv}")
        annotation_df = pd.DataFrame(final_unique_annotations)
        annotation_df.to_csv(output_annotations_csv, index=False, quoting=csv.QUOTE_ALL)
        print("Annotation CSV created successfully.")
    else:
        print("No annotations were generated.")