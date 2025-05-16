import os
import re
import pandas as pd
import csv
import json

# --- Constants for Annotation Labels (assuming these are defined globally as before) ---
LABEL_LOC_P = "LOC_P"
LABEL_LOC_C = "LOC_C"
LABEL_LOC_B = "LOC_B"
LABEL_HDR = "HDR"
LABEL_DATA = "DATA"
LABEL_BLANK = "BLANK"
LABEL_OTHER = "OTHER"
LABEL_TITLE = "TITLE"
LABEL_NOTE = "NOTE"
LABEL_LOC_DESCRIPTOR = "LOC_DESCRIPTOR"  # For rows like "PROVINCE / CITY / ..."


# --- Assume your other helper functions (xls_to_df, clean_value, extract_value,
# find_location_components, find_column_headers) are defined above this ---
# IMPORTANT:
# find_location_components should be modified to:
#   - Take `annotations_cache: dict` as a parameter.
#   - Update labels directly in this `annotations_cache` for the rows it processes.
#   - Return: (prov, city, brgy, next_search_start_idx, list_of_df_indices_it_scanned)
# find_column_headers should be modified to:
#   - Return: (found_bool, header_indices_dict, last_row_idx_of_header_block, list_of_actual_header_df_indices)

def main(
        df: pd.DataFrame,
        filename_for_ann: str,  # For annotation context
        sheetname_for_ann: str,  # For annotation context
        annotations_cache: dict,  # Pass the cache for the current sheet (dict: {row_idx: annotation_dict})
        debug: bool = False,
        start_row_index: int = 0,
        end_row_index: int = -1,
        debug_location: bool = False,
        debug_header: bool = False
) -> pd.DataFrame:
    """
    Main processing function to extract structured data and generate pseudo-annotations.
    It iterates through the DataFrame, identifying location components, column headers,
    and data rows, updating the annotations_cache with labels for each row.

    Args:
        df: The input DataFrame from an Excel sheet.
        filename_for_ann: Filename for annotation metadata.
        sheetname_for_ann: Sheet name for annotation metadata.
        annotations_cache: A dictionary to store/update annotations for the current sheet.
                           It will be pre-filled by the caller for all rows in the df.
        debug: General debug flag.
        start_row_index: DataFrame row index to start processing from.
        end_row_index: DataFrame row index to end processing at (-1 for end of DataFrame).
        debug_location: Specific debug flag for location component finding.
        debug_header: Specific debug flag for header finding.

    Returns:
        A new pandas DataFrame with structured data.
    """

    # Determine the final row index for processing
    # Ensure end_row_index is not out of bounds if specified
    max_row_index = len(df) if end_row_index == -1 else min(end_row_index, len(df))
    current_row_index = start_row_index

    # --- Annotation Cache Pre-initialization (Crucial) ---
    # The caller (`if __name__ == "__main__":`) should ideally pre-fill this cache
    # for all rows (start_row_index to max_row_index) with a default LABEL_OTHER or LABEL_BLANK.
    # This ensures every row has an entry that can be updated.
    # For robustness, we can add a check here, but it's better done by the caller.
    for r_idx_init in range(start_row_index, max_row_index):
        if r_idx_init not in annotations_cache:
            try:
                raw_cells_list_init = [str(cell) if not pd.isna(cell) else "" for cell in df.iloc[r_idx_init].tolist()]
                combined_text_init = "".join(c for c in raw_cells_list_init if c).strip()  # Check if row is empty
                annotations_cache[r_idx_init] = {
                    "filename": filename_for_ann, "sheetname": sheetname_for_ann,
                    "row_index": r_idx_init, "raw_cells_json": json.dumps(raw_cells_list_init),
                    "label": LABEL_BLANK if not combined_text_init else LABEL_OTHER
                }
            except IndexError:  # Should not happen if max_row_index is correct
                if debug: print(f"Warning: Row index {r_idx_init} out of bounds during cache pre-fill.")
                break

    processed_table_count = 0
    output_columns = ['Province', 'City/Municipality', 'Barangay',
                      'Street/Subdivision', 'Vicinity', 'Classification', 'ZV/SQM']
    structured_df = pd.DataFrame(columns=output_columns)

    LOCATION_PROXIMITY_WINDOW = 3
    HEADER_PROXIMITY_WINDOW = 3

    # State variables for current location context (will be updated by find_location_components)
    current_province = None
    current_city = None
    current_barangay = None

    # State variables for table processing (reset for each new table)
    current_header_indices = None
    # is_continuation_table = False # This logic might need revisiting based on how province changes
    prev_row_col1_val = None
    prev_row_vicinity_val = None
    current_table_col1_holder = None
    current_table_vicinity_holder = None
    current_table_all_other_vicinity_cache = None
    prev_row_was_all_other_type = False

    # --- Main Processing Loop ---
    while current_row_index < max_row_index:
        iteration_start_index = current_row_index  # To track progress if no table is formed

        # --- 1. Find Location Components ---
        # `find_location_components` will update labels in `annotations_cache` for the rows it scans.
        # It returns the determined P/C/B values, the next index to start searching from,
        # and the list of row indices it actually processed.
        temp_prov, temp_city, temp_brgy, loc_search_end_idx, _ = find_location_components(
            df, current_row_index, annotations_cache, filename_for_ann, sheetname_for_ann,
            proximity_window=LOCATION_PROXIMITY_WINDOW,
            current_province=current_province,  # Pass current state to allow filling gaps
            current_city=current_city,
            current_barangay=current_barangay,
            debug=debug_location
        )
        # Note: find_location_components is now responsible for updating the labels
        # in annotations_cache for the rows it processes.

        # Update current location state based on what find_location_components returned.
        # Prioritize newly found, non-empty values.
        if temp_prov: current_province = temp_prov
        if temp_city: current_city = temp_city
        if temp_brgy: current_barangay = temp_brgy

        if debug and (temp_prov or temp_city or temp_brgy):  # Log if any loc component was found/updated
            print(
                f"Location components after scan ending at df_row {loc_search_end_idx - 1}. Prov='{current_province}', City='{current_city}', Brgy='{current_barangay}'")

        # --- 2. Find Column Headers ---
        # Start searching for headers from where location component search left off.
        # `find_column_headers` returns: found_bool, header_indices_dict, last_row_idx_of_header_block, list_of_actual_header_df_indices
        headers_found, new_header_indices, header_block_last_row, actual_header_df_indices = find_column_headers(
            df, loc_search_end_idx,  # Start search from here
            proximity_window=HEADER_PROXIMITY_WINDOW,
            debug=debug_header
        )

        # Update annotations_cache for the identified header rows
        if headers_found:
            for r_idx_hdr in actual_header_df_indices:  # These are the specific DF row indices
                if r_idx_hdr < max_row_index:  # Boundary check
                    # The raw_cells_json was set during pre-initialization
                    # We only need to update the label if it's more specific
                    existing_ann = annotations_cache.get(r_idx_hdr)
                    if existing_ann:
                        priority = {LABEL_LOC_P: 5, LABEL_LOC_C: 5, LABEL_LOC_B: 5, LABEL_LOC_DESCRIPTOR: 4,
                                    LABEL_HDR: 3, LABEL_DATA: 2, LABEL_BLANK: 1, LABEL_OTHER: 0}
                        if priority.get(LABEL_HDR, -1) > priority.get(existing_ann["label"], -1):
                            annotations_cache[r_idx_hdr]["label"] = LABEL_HDR
                    # If not existing (shouldn't happen with pre-fill), it would be an error or need adding.
            if debug: print(
                f"Column headers found. Header block ends at df_row {header_block_last_row}. Actual header rows: {actual_header_df_indices}")

        # --- 3. Process Table Data or Advance ---
        # A table is considered to exist if headers were found.
        # Location context (current_province etc.) is carried over or updated.
        if headers_found:
            # A new table or continuation of a table is starting.
            # Update the current header indices for data extraction.
            current_header_indices = new_header_indices
            # Data rows start *after* the entire header block.
            data_processing_start_index = header_block_last_row + 1
            current_row_index = data_processing_start_index  # Set current_row_index to start of data

            processed_table_count += 1
            if debug:
                print(f'\n{"#" * 20} PROCESSING TABLE {processed_table_count} {"#" * 20}')
                print(f"Location context: P='{current_province}', C='{current_city}', B='{current_barangay}'")
                print(f"Using Headers: {current_header_indices}\n")

            # Reset states for the new table's data rows
            empty_row_streak = 0
            MAX_EMPTY_ROW_STREAK = 4
            prev_row_col1_val = None  # Reset for new table's data context
            prev_row_vicinity_val = None
            current_table_col1_holder = None
            current_table_vicinity_holder = None
            current_table_all_other_vicinity_cache = None
            prev_row_was_all_other_type = False

            # --- Inner loop: Process data rows for the current table ---
            while current_row_index < max_row_index and empty_row_streak < MAX_EMPTY_ROW_STREAK:
                current_data_row_series = df.iloc[current_row_index]
                # raw_cells_list_for_data_row = [str(cell) if not pd.isna(cell) else "" for cell in current_data_row_series.tolist()]
                # (raw_cells_json for this row is already in annotations_cache from pre-fill)

                # Extract cell values based on found header indices
                # Ensure robust access with boundary checks
                col1_val = None
                if current_header_indices.get('street_name_index') is not None and current_header_indices[
                    'street_name_index'] < len(current_data_row_series):
                    col1_val = current_data_row_series.iloc[current_header_indices['street_name_index']]

                vicinity_val = None
                vicinity_idx_config = current_header_indices.get('vicinity_index')
                if isinstance(vicinity_idx_config, int):
                    if vicinity_idx_config < len(current_data_row_series):
                        vicinity_val = current_data_row_series.iloc[vicinity_idx_config]
                elif isinstance(vicinity_idx_config, list):  # Handle potential merged vicinity columns
                    vic_str_parts = [str(current_data_row_series.iloc[idx]) for idx in vicinity_idx_config if
                                     idx is not None and idx < len(current_data_row_series)]
                    vic_str_parts = [s for s in vic_str_parts if s.lower() != 'nan']
                    vicinity_val = ', '.join(vic_str_parts) if vic_str_parts else None

                classification_val = None
                if current_header_indices.get('classification_index') is not None and current_header_indices[
                    'classification_index'] < len(current_data_row_series):
                    classification_val = current_data_row_series.iloc[current_header_indices['classification_index']]

                zv_sqm_val = None
                if current_header_indices.get('zv_sq_m_index') is not None and current_header_indices[
                    'zv_sq_m_index'] < len(current_data_row_series):
                    zv_sqm_val = current_data_row_series.iloc[current_header_indices['zv_sq_m_index']]

                if debug:
                    print(
                        f"\n  Raw data at df_row {current_row_index}: Col1='{col1_val}', Vicinity='{vicinity_val}', Class='{classification_val}', ZV='{zv_sqm_val}'")

                # --- Check for new location/header interrupting current table data ---
                # This uses a temporary cache for the check to avoid polluting the main one.
                temp_check_cache = {}
                temp_prov_check, temp_city_check, temp_brgy_check, temp_loc_idx_check, _ = find_location_components(
                    df, current_row_index, temp_check_cache, filename_for_ann, sheetname_for_ann, proximity_window=1,
                    debug=False
                )
                is_new_header_present_check, _, _, _ = find_column_headers(df, temp_loc_idx_check, proximity_window=1,
                                                                           debug=False)

                combined_class_zv_str = str(classification_val) + str(zv_sqm_val)
                is_empty_class_zv = not clean_value(combined_class_zv_str)

                if is_empty_class_zv and (any([temp_prov_check, temp_city_check,
                                               temp_brgy_check]) or is_new_header_present_check):  # Modified: OR, as new header alone can break
                    if debug: print(
                        f"  New location/header found mid-table at df_row {current_row_index}. Ending current table.")
                    # The label for current_row_index in annotations_cache (likely OTHER/BLANK from init)
                    # will be updated correctly in the *next* outer loop iteration when it's processed as loc/header.
                    break  # End current table processing, current_row_index will be re-evaluated by outer loop.

                # --- Validate data row (is it empty or skippable?) ---
                # A row is invalid if its raw content is empty or if key fields (class & ZV) are empty.
                current_row_raw_text_content = "".join(
                    str(s) for s in current_data_row_series.tolist() if not pd.isna(s)).strip()

                is_class_empty = pd.isnull(classification_val) or str(
                    classification_val).strip().lower() == 'nan' or str(classification_val).strip() == ''
                is_zv_empty = pd.isnull(zv_sqm_val) or str(zv_sqm_val).strip().lower() == 'nan' or str(
                    zv_sqm_val).strip() == ''

                # Skip if the entire row is effectively blank OR if both class and ZV are empty
                if not current_row_raw_text_content or (is_class_empty and is_zv_empty):
                    if debug: print(f"  Skipping empty/invalid data row {current_row_index}.")
                    annotations_cache[current_row_index]["label"] = LABEL_BLANK  # Update label in cache
                    current_row_index += 1
                    empty_row_streak += 1
                    continue

                # Specific skip for "nan" classification if ZV doesn't look like a number
                if str(classification_val).strip().lower() == 'nan' and not (isinstance(zv_sqm_val, (int, float)) or (
                        isinstance(zv_sqm_val, str) and zv_sqm_val.replace('.', '', 1).isdigit())):
                    if debug: print(
                        f"  Skipping row {current_row_index} due to 'nan' classification and non-numeric ZV.")
                    annotations_cache[current_row_index][
                        "label"] = LABEL_OTHER  # Update label in cache (or BLANK if raw is empty)
                    current_row_index += 1
                    continue  # Don't increment empty_row_streak here as per original logic, could be data just not extractable.

                # Skip rows that are just dashes
                def is_dash_str(val):
                    return isinstance(val, str) and re.fullmatch(r"-+", val.strip()) is not None

                if sum(is_dash_str(v) for v in
                       [col1_val, vicinity_val, classification_val, zv_sqm_val]) >= 2:  # Relaxed from 3
                    if debug: print(f"  Skipping dashed row {current_row_index}.")
                    annotations_cache[current_row_index]["label"] = LABEL_BLANK  # Treat as blank for annotation
                    current_row_index += 1
                    empty_row_streak += 1
                    continue

                # If we reach here, it's considered a data row by the current logic.
                empty_row_streak = 0  # Valid data found, reset streak
                annotations_cache[current_row_index]["label"] = LABEL_DATA  # Update label in cache

                # --- Handle "ALL OTHER" street logic and carry-over for empty Col1/Vicinity ---
                # (Your existing logic for this, ensure it uses the locally extracted col1_val, vicinity_val)
                # This logic modifies col1_val and vicinity_val *before* they are cleaned for output.
                is_col1_empty_or_nan = pd.isna(col1_val) or not str(col1_val).strip()
                if not is_col1_empty_or_nan: current_table_all_other_vicinity_cache = None  # Reset cache if new col1

                current_col1_is_all_other_type = False
                if isinstance(col1_val, str):
                    col1_upper_stripped = col1_val.strip().upper()
                    if col1_upper_stripped.startswith("ALL OTHER") or col1_upper_stripped.startswith("ALL LOTS"):
                        current_col1_is_all_other_type = True

                if is_col1_empty_or_nan:
                    col1_val = current_table_col1_holder  # Simplified from original, check if `is_continuation_table` logic is needed here
                else:
                    current_table_col1_holder = col1_val

                is_vicinity_empty_or_nan = pd.isna(vicinity_val) or not str(vicinity_val).strip()
                if is_vicinity_empty_or_nan:
                    vicinity_val = current_table_vicinity_holder  # Simplified
                else:
                    current_table_vicinity_holder = vicinity_val

                # prev_row_was_all_other_type update (simplified)
                if prev_row_col1_val and (isinstance(prev_row_col1_val, str) and (prev_row_col1_val.strip().upper().startswith("ALL OTHER") or prev_row_col1_val.strip().upper().startswith("ALL LOTS"))):
                    prev_row_was_all_other_type = True
                else:
                    prev_row_was_all_other_type = False

                if current_col1_is_all_other_type:
                    if not is_vicinity_empty_or_nan:
                        current_table_all_other_vicinity_cache = vicinity_val
                    elif current_table_all_other_vicinity_cache or prev_row_was_all_other_type:  # check if current col1 is empty AND prev was all other
                        if is_col1_empty_or_nan and prev_row_was_all_other_type:
                            vicinity_val = current_table_all_other_vicinity_cache if current_table_all_other_vicinity_cache else prev_row_vicinity_val  # Try cache, then previous row's vicinity
                        else:
                            vicinity_val = current_table_all_other_vicinity_cache
                    else:
                        vicinity_val = ''  # Fallback for ALL OTHER with no vicinity info
                elif is_col1_empty_or_nan and current_table_all_other_vicinity_cache and prev_row_was_all_other_type:
                    # If col1 is empty, under an "ALL OTHER" context from previous row.
                    vicinity_val = current_table_all_other_vicinity_cache

                # --- Append data to structured_df ---
                new_row_data = {
                    'Province': current_province,
                    'City/Municipality': current_city,
                    'Barangay': current_barangay,
                    'Street/Subdivision': clean_value(col1_val, feature=True),
                    'Vicinity': clean_value(vicinity_val, feature=True),
                    'Classification': clean_value(classification_val, feature=True),
                    'ZV/SQM': clean_value(zv_sqm_val)  # ZV/SQM is numeric or empty string by clean_value
                }
                structured_df.loc[len(structured_df)] = new_row_data

                if debug:
                    print(f"  Appended data: {new_row_data}")
                    print("\n  -------")

                # Update previous row values for next iteration's carry-over logic
                prev_row_col1_val = col1_val  # Store before cleaning for accurate carry-over
                prev_row_vicinity_val = vicinity_val

                current_row_index += 1
            # --- End of inner data row loop ---
            continue  # To the next iteration of the outer while loop (for a new table search)

        else:  # Headers not found for the current block starting at `loc_search_end_idx`
            if debug: print(
                f"  No headers found after location scan (or no location found). Advancing scan from df_row {iteration_start_index}.")
            # The labels for rows scanned by find_location_components and find_column_headers (if any)
            # are already updated in annotations_cache.
            # We need to ensure current_row_index advances past the scanned region to avoid getting stuck.
            current_row_index = max(iteration_start_index + 1, loc_search_end_idx)
            # If find_column_headers scanned further, that's implicitly handled as loc_search_end_idx would be its start.

    if debug:
        print(f"\nTotal tables processed: {processed_table_count}")
    return structured_df

# --- Main execution block (if __name__ == "__main__":) ---
# This should be very similar to the one provided in the previous full code example.
# Key change: it creates `current_sheet_annotations_cache = {}` for each sheet
# and passes it to `main`. After `main` returns, it collects the values from this
# cache into `all_annotations_globally_list`.