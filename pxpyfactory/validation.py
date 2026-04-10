import pandas as pd
from datetime import datetime


# Format a timestamp as a string.
# Args:
#     timestamp: datetime object, Unix timestamp, or None (uses current time)
# Returns:
#     Formatted string: "YYYYMMDD HH:MM"
def get_time_formatted(timestamp=None):
    if timestamp is None:
        return_time = datetime.now()
    elif isinstance(timestamp, datetime):
        # If datetime has timezone info, convert to local time
        if timestamp.tzinfo is not None:
            return_time = timestamp.astimezone()
        else:
            return_time = timestamp
    else:
        # Convert timestamp to local time (assumes timestamp is Unix time)
        return_time = datetime.fromtimestamp(float(timestamp))
    return return_time.strftime("%Y%m%d %H:%M")


# Check if a value is valid and return it, or return None if invalid.
# Args:
#     value: Value to check
#     full: If True, also treats '-', '.', '..' as invalid
# Returns:
#     The value if valid, None otherwise
def valid_value_or_none(value, full=False):
    # Handle None first
    if value is None:
        return None
    
    # If value is a list, tuple, or Series - check early to avoid issues with 'in' operator
    if isinstance(value, (list, tuple, pd.Series)):
        if len(value) == 0 or (len(value) == 1 and pd.isnull(value[0])):
            return None
        return value
    
    # Handle empty strings and dicts
    if value == '' or value == {}:
        return None
    
    # Handle string placeholders
    if isinstance(value, str):
        if value.lower() in ['none', 'null', 'nan', 'nat']:
            return None
        if full and value in ['-', '.', '..']:
            return None
    
    # Handle NaN for numeric types
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass  # Not a type that can be NaN
    
    # Handle datetime
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.replace(tzinfo=None)
        time_from = datetime(2020, 1, 1)
        time_to = datetime(2099, 12, 31)
        if time_from <= value <= time_to:
            return get_time_formatted(value)
        return None  # Outside valid range
    
    # If we got here, value is valid
    return value


# Check if a value is valid (not None, not empty, etc.).
def valid_value(value, full=False):
    return not valid_value_or_none(value, full=full) is None


# Check if two values are the same after normalization.
def same_value(value1, value2):
    return valid_value_or_none(value1, full=True) == valid_value_or_none(value2, full=True)


# Returns the first non-null value in a row.
# If all values are null, returns an empty string.
def get_first_notnull(row):
    for value in row:
        if valid_value(value):
            return value
        else:
            continue
    return ''


# Check if a list is None, empty, or contains only empty/NAN values.
def is_list_empty(check_list):
    if check_list is None:
        return True
    elif check_list in ([''], ['NAN']):
        return True
    elif len(check_list) == 0:
        return True
    else:
        return False
