import json
from datetime import datetime, timezone


def generate_sqa_content(self, table_id, stub_list, heading_list, data_list, values_dict, contvariable, language="no"):
    """
    Generate the content for a .sqa (saved query attributes) file.
    
    Args:
        table_id: The table reference/ID
        stub_list: List of stub variables
        heading_list: List of heading variables
        data_list: List of data/content variables
        values_dict: Dictionary with unique values for each variable
        contvariable: Content variable name
        input_params: DataFrame with SQ parameters (KEYWORD, VALUE) for column selection
        rename_map: Dictionary mapping original column names to renamed ones
        language: Language code (default "no")
    
    Returns:
        JSON string for .sqa file
    """
    # Rename KEYWORDs in input_params to match renamed columns
    # Build rename map from metadata
    rename_map = {}
    for _, row in self.table_meta_rename.iterrows():
        keyword = row['KEYWORD']
        new_name = row['VALUE']
        rename_map[keyword] = new_name
    
    input_params = self.table_meta_sq # DataFrame with SQ parameters - contains KEYWORD = the column name, VALUE number of rows to show (from last)
    input_params['KEYWORD'] = input_params['KEYWORD'].map(lambda x: rename_map.get(x, x))
    
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
            # Check if there's a constraint from input_params
            constraint_row = input_params[input_params['KEYWORD'] == var]
            
            if not constraint_row.empty:
                # Get number of last values to include
                num_values = int(constraint_row['VALUE'].iloc[0])
                # Take last N values from values_dict
                all_values = values_dict[var]
                selected_indices = list(range(max(0, len(all_values) - num_values), len(all_values)))
                value_codes = [str(i) for i in selected_indices]
            else:
                # Include all values
                value_codes = [str(i) for i in range(len(values_dict[var]))]
            
            selection.append({
                "VariableCode": var,
                "CodeList": None,
                "ValueCodes": value_codes
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
