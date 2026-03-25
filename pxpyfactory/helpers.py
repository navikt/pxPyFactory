import sys
import pxpyfactory.validation


# Set the input arguments for the module to use
input_args = {}


def set_input_args():
    """Parse command line arguments into global input_args dictionary."""
    global input_args
    sys_argv = sys.argv[1:] if len(sys.argv) > 1 else []
    input_args = {}
    for arg in sys_argv:
        if '=' in arg:
            key, value = arg.split('=', 1)  # Split only on first '='
            # Auto-convert common types
            if value.lower() in ('true', 'false'):
                value = value.lower() == 'true'
            elif value.isdigit():
                value = int(value)
            input_args[key] = value
        else:
            input_args[arg] = True  # Flag-style argument
    print_filter(f"Input arguments: {input_args}", 0)


def get_input_args(type=None):
    """Get command line arguments. If type is None, returns all args, otherwise returns specific arg."""
    if type == None:
        return input_args
    else:
        return input_args.get(type, None)


def print_filter(output, priority_level=0):
    """Print output only if priority_level is <= print level from command line args."""
    print_level = get_input_args('print')
    try:
        print_level = int(print_level)
    except (ValueError, TypeError):
        print_level = 0
    if priority_level <= print_level:
        print(output)


def shorten_tableid(tableid):
    """
    Shorten table reference to max 20 chars (excluding separators) by truncating each part.
    Removes '_' and '-' separators from the tableid.
    """
    import re
    tableid_str = str(tableid)
    text_parts = re.split(r'[_-]', tableid_str)  # Split by '_' and '-' to get text parts only
    
    # Check if total length (without separators) exceeds 20
    total_length = sum(len(p) for p in text_parts)
    if total_length <= 20:
        # Still remove separators even if not too long
        return ''.join(text_parts)
    
    max_chars = 20 // len(text_parts)  # Calculate max chars per part by floor division
    truncated_parts = [p[:max_chars] for p in text_parts]  # Truncate each text part
    
    return ''.join(truncated_parts)


def prep_list_from_string(in_string, separator=',', to_upper=True, split_part=0):
    """
    Convert a delimited string to a list with optional transformations.
    Args:
        in_string: String to convert
        separator: Delimiter to split on (default: ',')
        to_upper: Convert to uppercase (default: True)
        split_part: If not None, extract this part after splitting by '#'
    Returns:
        List of processed strings
    """
    if in_string is None:
        out_list = []
    elif isinstance(in_string, str):
        out_list = [_prep_list_from_string_mod(sub, to_upper, split_part) for sub in in_string.split(separator)]
    elif isinstance(in_string, (int, float)):
        out_list = [str(in_string)]
    else:
        out_list = []
    return out_list


def _prep_list_from_string_mod(substring, to_upper, split_part):
    """Helper function for prep_list_from_string to process individual substring."""
    if split_part is not None:
        try:
            substring = substring.split('#')[split_part]
        except Exception:
            substring = None
    if substring != None:
        substring = substring.strip()
        if to_upper:
            substring = substring.upper()
    return substring
