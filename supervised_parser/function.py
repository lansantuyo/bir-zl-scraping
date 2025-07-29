import os
import re
import pandas as pd
import numpy as np
import json
import csv  # Keep for potential future use


# ==============================================================================
# --- PARSER BLUEPRINT & PROFILE EXAMPLE ---
# ==============================================================================

# This script is a hybrid parser driven by a JSON profile.
#
# Create a 'profile.json' file with a structure like this:
#
# {
#   "profile_name": "RDO 39 - Valenzuela City (Hybrid Example)",
#   "file_settings": {
#     "sheet_identifier": "Sheet1",
#     "data_starts_after_row": 5,
#     "data_stops_after_row": 250
#   },
#   "general_rules": {
#     "data_start_marker": { "enabled": true, "text": "SCHEDULE OF ZONAL VALUES" },
#     "on_index_error": "halt",
#     "carry_down_columns": [3, 5]
#   },
#   "location_strategy": {
#     "mode": "automatic",
#     "automatic_settings": { "proximity_window": 3 },
#     "manual_definitions": [
#       {
#         "match_pattern": "PROVINCE/CITY",
#         "value_source": {
#           "method": "regex_from_next_row",
#           "pattern": "(?<province>METRO MANILA)\\s*:\\s*(?<city>VALENZUELA CITY)"
#         }
#       }
#     ]
#   },
#   "data_extraction_strategy": {
#     "column_mapping": {
#       "province": null, "city": null, "barangay": null,
#       "street": 3, "vicinity": 5, "classification": 6, "zv_sqm": 7
#     },
#     "row_validators": [
#       { "type": "column_not_empty", "index": 7 },
#       { "type": "column_is_numeric", "index": 7 }
#     ],
#     "column_cleaners": [
#       { "index": 6, "apply": ["trim_whitespace", "to_uppercase"] },
#       { "index": 7, "apply": ["remove_commas", "to_float"] }
#     ]
#   }
# }
#

# ==============================================================================
# --- DATA CLEANING LIBRARY ---
# ==============================================================================

def _clean_trim_whitespace(value):
    return str(value).strip()


def _clean_to_uppercase(value):
    return str(value).upper()


def _clean_remove_commas(value):
    return str(value).replace(',', '')


def _clean_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


CLEANER_LIBRARY = {
    "trim_whitespace": _clean_trim_whitespace,
    "to_uppercase": _clean_to_uppercase,
    "remove_commas": _clean_remove_commas,
    "to_float": _clean_to_float,
}


def _apply_cleaners(value, column_index, cleaner_definitions):
    """Applies a sequence of cleaning functions to a value."""
    for definition in cleaner_definitions:
        if definition.get("index") == column_index:
            for cleaner_name in definition.get("apply", []):
                cleaner_func = CLEANER_LIBRARY.get(cleaner_name)
                if cleaner_func:
                    value = cleaner_func(value)
    return value


# ==============================================================================
# --- FILE AND DATAFRAME HANDLING ---
# ==============================================================================

def _load_dataframe(excel_path: str, file_settings: dict) -> pd.DataFrame:
    """Reads an Excel file into a DataFrame based on profile settings."""
    sheet_id = file_settings.get("sheet_identifier", 0)
    engine = 'xlrd' if excel_path.lower().endswith('.xls') else 'openpyxl'

    try:
        return pd.read_excel(excel_path, sheet_name=sheet_id, header=None, engine=engine)
    except Exception as e:
        print(f"Error reading Excel file '{excel_path}': {e}")
        return None


# ==============================================================================
# --- AUTOMATIC MODE (Original Logic) ---
# ==============================================================================
# The functions from your original script are preserved here for the "automatic" mode.
# I've prefixed them with _ to indicate they are internal parts of the engine.

# --- Original Regex Constants (for automatic mode) ---
_STREET_HEADER_PATTERN = re.compile(
    r"(S\s*T\s*R\s*E\s*E\s*T\s*N\s*A\s*M\s*E|S\s*U\s*B\s*D\s*I\s*V\s*I\s*S\s*I\s*O\s*N|C\s*O\s*N\s*D\s*O\s*M\s*I\s*N\s*I\s*U\s*M)",
    re.IGNORECASE)
_VICINITY_HEADER_PATTERN = re.compile(r"V\s*I\s*C\s*I\s*N\s*I\s*T\s*Y", re.IGNORECASE)
_CLASSIFICATION_HEADER_PATTERN = re.compile(
    r"CLASS(?:IFICATION)?|C\s*L\s*A\s*S\s*S\s*I\s*F\s*I\s*C\s*A\s*T\s*I\s*O\s*N", re.IGNORECASE | re.DOTALL)
_ZV_SQM_HEADER_PATTERN = re.compile(
    r"\d+(?:ST|ND|RD|TH)\s+(?:REVISION|Rev)(?:.*Z\.?V\.?.*SQ.*M\.?)?|(?:\d+(?:ST|ND|RD|TH)\s+REVISION|Rev\s+ZV\s+/?.*SQ\.?\s*M\.?)|(?:Z|2)\.?V\.?.*SQ.*M\.?|FINAL",
    re.IGNORECASE)
_PROVINCE_PATTERN = re.compile(r"Province\s*(?::|\s|of)?\s*(.*)", re.IGNORECASE)
_CITY_PATTERN = re.compile(r"(?:(?!City,)(?:City|Municipality))(?:\s*\/\s*(?:City|Municipality))?\s*[:\s]?\s*(.+)",
                           re.IGNORECASE)
_BARANGAY_PATTERN = re.compile(r"(?:Barangays|Zone|Barangay)(?:\s*\/\s*(?:Barangays|Zone|Barangay))?\s*[:\s]?\s*(.+)",
                               re.IGNORECASE)
_COMBINED_LOCATION_PATTERN = re.compile(r"PROVINCE\s*/\s*CITY\s*/\s*MUNICIPALITY\s*/\s*BARANGAYS", re.IGNORECASE)


def _clean_value_auto(value):
    """Original cleaning function for automatic mode."""
    try:
        return round(float(value), 3)
    except (ValueError, TypeError):
        value = str(value).strip()
        if value.lower() == 'nan': return ''
        return value


def _find_location_components_auto(df, data, index, proximity_window=3):
    # This is a simplified placeholder for your original complex function.
    # In a real implementation, the full logic of your find_location_components would go here.
    # For this example, we'll assume a very basic search.
    # --- PASTE your full 'find_location_components' logic here ---
    print(f"AUTOMATIC: Searching for location near row {index}...")
    # This is just a dummy implementation for demonstration
    row_text = ' '.join(map(str, data[index]))
    province_match = _PROVINCE_PATTERN.search(row_text)
    city_match = _CITY_PATTERN.search(row_text)
    if province_match or city_match:
        province = province_match.group(1) if province_match else None
        city = city_match.group(1) if city_match else None
        return province, city, None, index + 1
    return None, None, None, index


def _find_column_headers_auto(df, data, index, proximity_window=3):
    # This is a simplified placeholder for your original complex function.
    # --- PASTE your full 'find_column_headers' logic here ---
    print(f"AUTOMATIC: Searching for headers near row {index}...")
    # This is just a dummy implementation for demonstration
    row_text = ' '.join(map(str, data[index]))
    if "STREET" in row_text.upper() and "VICINITY" in row_text.upper():
        headers = {'street_name_index': 1, 'vicinity_index': 2, 'classification_index': 3, 'zv_sq_m_index': 4}
        return True, headers, index + 1, {index}
    return False, None, index, set()


def _run_automatic_mode(df: pd.DataFrame, profile: dict) -> pd.DataFrame:
    """
    Executes the original, fully automated parsing logic.
    This function is a wrapper around your 'mainv3' code.
    """
    print("\n--- Running in AUTOMATIC mode ---")
    # --- PASTE and ADAPT your full 'mainv3' logic here ---
    # The original 'mainv3' loop would be placed here.
    # It would call the original helper functions like _find_location_components_auto
    # and _find_column_headers_auto.

    print("Automatic mode finished. (This is a placeholder for the original script's logic).")
    # For demonstration, we'll return an empty DataFrame.
    return pd.DataFrame(
        columns=['Province', 'City/Municipality', 'Barangay', 'Street/Subdivision', 'Vicinity', 'Classification',
                 'ZV/SQM'])


# ==============================================================================
# --- MANUAL MODE (Profile-Driven Logic) ---
# ==============================================================================

def _is_valid_data_row(row: pd.Series, validators: list) -> bool:
    """Checks if a row meets all data validation criteria from the profile."""
    for validator in validators:
        idx = validator.get("index")
        try:
            cell_value = row.iloc[idx]
        except IndexError:
            return False  # Index doesn't exist, so it's not a valid data row

        if validator["type"] == "column_not_empty":
            if pd.isna(cell_value) or str(cell_value).strip() == "":
                return False
        elif validator["type"] == "column_is_numeric":
            try:
                float(str(cell_value).replace(',', ''))
            except (ValueError, TypeError):
                return False
    return True


def _process_location_row(row_text: str, next_row_text: str, definitions: list, current_state: dict):
    """Checks if a row is a location row and updates the state."""
    for definition in definitions:
        if re.search(definition["match_pattern"], row_text, re.IGNORECASE):
            source = definition["value_source"]
            target_text = row_text if source["method"] != "regex_from_next_row" else next_row_text

            match = re.search(source["pattern"], target_text, re.IGNORECASE)
            if match:
                # Update state with named capture groups from the regex
                current_state.update(match.groupdict())
                return True  # It was a location row
    return False


def _run_manual_mode(df: pd.DataFrame, profile: dict) -> pd.DataFrame:
    """Executes the new, profile-driven stateful parsing logic."""
    print("\n--- Running in MANUAL mode ---")

    # Extract settings from profile for easier access
    rules = profile.get("general_rules", {})
    file_settings = profile.get("file_settings", {})
    loc_defs = profile["location_strategy"]["manual_definitions"]
    data_defs = profile["data_extraction_strategy"]

    # Initialize state
    current_state = {"province": None, "city": None, "barangay": None}
    carry_down_state = {}
    output_records = []

    start_marker = rules.get("data_start_marker", {})
    processing_started = not start_marker.get("enabled", False)

    start_row = file_settings.get("data_starts_after_row", 0)
    stop_row = file_settings.get("data_stops_after_row", len(df))

    # Main processing loop
    for index, row in df.iterrows():
        if index < start_row: continue
        if index > stop_row: break

        row_text = ' '.join(row.dropna().astype(str))
        next_row_text = ' '.join(df.iloc[index + 1].dropna().astype(str)) if index + 1 < len(df) else ""

        if not processing_started:
            if start_marker.get("text", "") in row_text:
                processing_started = True
            continue

        was_location_row = _process_location_row(row_text, next_row_text, loc_defs, current_state)
        if was_location_row:
            continue

        is_data = _is_valid_data_row(row, data_defs["row_validators"])
        if is_data:
            record = {}
            mapping = data_defs["column_mapping"]

            # This is the user-defined carry-down logic.
            # 1. Update carry_down_state with any new non-empty values from this row
            for col_idx in rules.get("carry_down_columns", []):
                if col_idx < len(row) and not pd.isna(row.iloc[col_idx]) and str(row.iloc[col_idx]).strip() != '':
                    carry_down_state[col_idx] = row.iloc[col_idx]

            for dest_field, src_index in mapping.items():
                value = None
                if src_index is None:
                    value = current_state.get(dest_field)
                else:
                    try:
                        value = row.iloc[src_index]
                        # 2. Apply carry-down: if the current value is empty, use the carried-down one
                        if (pd.isna(value) or str(value).strip() == '') and src_index in carry_down_state:
                            value = carry_down_state[src_index]

                    except IndexError:
                        if rules.get("on_index_error") == "halt":
                            raise IndexError(
                                f"Profile Error: Column index {src_index} is out of bounds for the file at row {index}.")
                        value = None  # Or log and continue

                # Apply cleaners defined in the profile
                cleaned_value = _apply_cleaners(value, src_index, data_defs.get("column_cleaners", []))
                record[dest_field] = cleaned_value

            output_records.append(record)

    return pd.DataFrame(output_records)


# ==============================================================================
# --- MAIN DISPATCHER ---
# ==============================================================================

def run_parser(excel_path: str, profile_path: str) -> pd.DataFrame:
    """
    Main entry point. Loads an Excel file and a JSON profile, then
    dispatches to the correct parsing mode.
    """
    print(f"Loading profile: {profile_path}")
    try:
        with open(profile_path, 'r') as f:
            profile = json.load(f)
    except FileNotFoundError:
        print(f"FATAL: Profile file not found at '{profile_path}'")
        return None
    except json.JSONDecodeError as e:
        print(f"FATAL: Invalid JSON in profile file '{profile_path}': {e}")
        return None

    print(f"Loading Excel file: {excel_path}")
    df = _load_dataframe(excel_path, profile.get("file_settings", {}))
    if df is None:
        return None

    strategy = profile.get("location_strategy", {}).get("mode", "automatic")

    if strategy == "automatic":
        return _run_automatic_mode(df, profile)
    elif strategy == "manual":
        return _run_manual_mode(df, profile)
    else:
        print(f"FATAL: Invalid mode '{strategy}' in profile. Must be 'automatic' or 'manual'.")
        return None


if __name__ == '__main__':
    # --- Example Usage ---
    # To run this, you need:
    # 1. An Excel file named 'sample.xlsx'
    # 2. A profile file named 'profile.json'

    # Create a dummy Excel file for testing
    data = [
        ['REPUBLIC OF THE PHILIPPINES'],
        ['DEPARTMENT OF FINANCE'],
        ['BUREAU OF INTERNAL REVENUE'],
        [],
        ['SUBJECT: ZONAL VALUES'],
        ['Location Info:', 'PROVINCE/CITY', 'METRO MANILA:VALENZUELA CITY'],
        ['', 'BARANGAY/S', ': Marulas'],
        [],
        ['SCHEDULE OF ZONAL VALUES'],
        ['', 'Column 2', 'Street Name', 'Vicinity', 'Classification', 'ZV/SQ.M.'],
        ['', '', 'P.R. VALENZUELA', 'FROM A. DEATO TO...', 'CR', '8,000.00'],
        ['', '', '', 'FROM T. DEATO TO...', 'RR', '5,000.00'],
        ['', '', 'MACARTHUR HIGHWAY', '', 'CR', '15,000.00'],
        ['', '', '', '', 'RR', '10,000.00'],
        ['', '', 'ALL OTHER STREETS', '', 'CR', '4,000.00'],
        ['', '', '', '', 'RR', '2,500.00'],
        [],
        ['--- END ---']
    ]
    dummy_df = pd.DataFrame(data)
    dummy_df.to_excel("sample.xlsx", index=False, header=False)

    # Create a dummy profile file for testing
    profile_data = {
        "profile_name": "Test Profile for Manual Mode",
        "file_settings": {"sheet_identifier": 0},
        "general_rules": {
            "data_start_marker": {"enabled": True, "text": "SCHEDULE OF ZONAL VALUES"},
            "on_index_error": "halt",
            "carry_down_columns": [2]
        },
        "location_strategy": {
            "mode": "manual",
            "manual_definitions": [
                {
                    "match_pattern": "PROVINCE/CITY",
                    "value_source": {"method": "regex_from_next_row",
                                     "pattern": "(?<province>METRO MANILA):(?<city>VALENZUELA CITY)"}
                },
                {
                    "match_pattern": "BARANGAY/S",
                    "value_source": {"method": "value_after_colon", "pattern": ":\\s*(?<barangay>.*)"}
                }
            ]
        },
        "data_extraction_strategy": {
            "column_mapping": {
                "province": None, "city": None, "barangay": None,
                "street": 2, "vicinity": 3, "classification": 4, "zv_sqm": 5
            },
            "row_validators": [
                {"type": "column_not_empty", "index": 5},
                {"type": "column_not_empty", "index": 4}
            ],
            "column_cleaners": [
                {"index": 5, "apply": ["remove_commas", "to_float"]}
            ]
        }
    }
    with open("profile.json", "w") as f:
        json.dump(profile_data, f, indent=2)

    print("Created dummy 'sample.xlsx' and 'profile.json' for demonstration.")

    # Run the parser
    final_dataframe = run_parser(excel_path="sample.xlsx", profile_path="profile.json")

    if final_dataframe is not None:
        print("\n--- PARSING COMPLETE ---")
        print(final_dataframe.to_string())
        # final_dataframe.to_csv("output.csv", index=False)
        # print("\nOutput saved to output.csv")