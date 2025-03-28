import re

def convert_to_numeric(value):
    """Attempts to convert a value to a float or int; returns original value if conversion fails."""
    try:
        num = float(value)
        return int(num) if num.is_integer() else num  # Convert to int if whole number
    except (ValueError, TypeError):
        return value  # Return original if not a number

def is_extra_whitespace(df1_value, df2_value):
    """Check if values are the same except for leading/trailing spaces."""
    if isinstance(df1_value, (int, float)) or isinstance(df2_value, (int, float)):
        return False  # Ignore numeric values for this check
    return df1_value.strip() == df2_value.strip() and df1_value != df2_value

def is_case_difference(df1_value, df2_value):
    """Check if values only differ in letter case."""
    if isinstance(df1_value, (int, float)) or isinstance(df2_value, (int, float)):
        return False  # Ignore numeric values for this check
    return df1_value.lower() == df2_value.lower() and df1_value != df2_value

def is_numeric_rounding(df1_value, df2_value):
    """Check if numeric values are equal when rounded to 2 decimal places."""
    df1_value, df2_value = convert_to_numeric(df1_value), convert_to_numeric(df2_value)
    if isinstance(df1_value, (int, float)) and isinstance(df2_value, (int, float)):
        return round(df1_value, 2) == round(df2_value, 2) and df1_value != df2_value
    return False  # Not a numeric difference

def is_missing_value(df1_value, df2_value):
    """Check if one value is missing while the other is present."""
    return (df1_value in ['', None] and df2_value not in ['', None]) or (df1_value not in ['', None] and df2_value in ['', None])

def is_fill_forward_issue(df1_value, df2_value):
    """Check if values are repeated inappropriately."""
    return df1_value == df2_value and df1_value != ''

def is_header_included(df1_value, df2_value):
    """Check if a header mistakenly appears in the data."""
    headers = {"City/Municipality", "Street Name", "Vicinity", "Classification", "ZV/SQM"}
    return df1_value in headers or df2_value in headers

def is_cutoff_difference(df1_value, df2_value):
    """Check if one value is a substring of the other but has extra characters."""
    df1_value, df2_value = str(df1_value).strip(), str(df2_value).strip()
    
    # Ignore numeric values since substring logic does not apply to numbers
    if isinstance(convert_to_numeric(df1_value), (int, float)) or isinstance(convert_to_numeric(df2_value), (int, float)):
        return False

    return (df1_value in df2_value or df2_value in df1_value) and len(df1_value) != len(df2_value)
