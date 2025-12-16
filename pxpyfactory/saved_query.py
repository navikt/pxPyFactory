import json
from datetime import datetime, timezone


def generate_sqa_content(table_id, stub_list, heading_list, data_list, values_dict, language="no"):
    """
    Generate the content for a .sqa (saved query attributes) file.
    
    Args:
        table_id: The table reference/ID
        stub_list: List of stub variables
        heading_list: List of heading variables
        data_list: List of data/content variables
        values_dict: Dictionary with unique values for each variable
        language: Language code (default "no")
    
    Returns:
        JSON string for .sqa file
    """
    contvariable = "STAT_VAR"
    
    # Build selection array with all variables
    selection = []
    
    # Add content variable (STAT_VAR) with data columns as values
    selection.append({
        "VariableCode": contvariable,
        "CodeList": None,
        "ValueCodes": [str(i) for i in range(len(data_list))]
    })
    
    # Add all other variables (stub + heading)
    all_variables = heading_list + stub_list
    for var in all_variables:
        if var in values_dict:
            # Generate index-based value codes (0, 1, 2, ...)
            value_codes = [str(i) for i in range(len(values_dict[var]))]
            selection.append({
                "VariableCode": var,
                "CodeList": None,
                "ValueCodes": value_codes  # Try with None to select all values
            })
    
    # Build the complete structure
    sqa_structure = {
        "Id": "",
        "Selection": {
            "Selection": selection,
            "Placement": {
                "Heading": [contvariable] + heading_list,
                "Stub": stub_list
            }
        },
        "Language": language,
        "TableId": table_id,
        "OutputFormat": 2,
        "OutputFormatParams": []
    }
    
    return json.dumps(sqa_structure, indent=4, ensure_ascii=False)


def generate_sqs_content():
    """
    Generate the content for a .sqs (saved query statistics) file.
    
    Returns:
        JSON string for .sqs file
    """
    current_time = datetime.now(timezone.utc).isoformat()
    
    sqs_structure = {
        "Created": current_time,
        "LastUsed": current_time,
        "UsageCount": 1
    }
    
    return json.dumps(sqs_structure, indent=4, ensure_ascii=False)
