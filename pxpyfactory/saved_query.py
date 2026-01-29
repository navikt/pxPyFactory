import json
from datetime import datetime, timezone
import pxpyfactory.utils
# _____________________________________________________________________________
# Creates simple content for an .sqs (saved query statistics) file
# Retruns JSON string for .sqs file
def generate_sqs_content():
    current_time = datetime.now(timezone.utc).isoformat()
    sqs_structure = {
        "Created": current_time,
        "LastUsed": current_time,
        "UsageCount": 1
    }
    return json.dumps(sqs_structure, indent=4, ensure_ascii=False)

# _____________________________________________________________________________
# Creates content for an .sqa (saved query attributes) file
# Retruns JSON string for .sqa file
def generate_sqa_content(self, table_id, stub_list, heading_list, data_list, values_dict, contvariable, language="no"):

    # DataFrame with SQ parameters - contains KEYWORD = the column name, VALUE number of rows to show (from last)
    self.table_meta_sq['KEYWORD'] = self.table_meta_sq['KEYWORD'].map(lambda x: self.rename_map.get(x, x))
    
    # Build selection array with all variables
    selection = []
    

    # Add all other variables (stub + heading)
    all_variables = [contvariable] + heading_list + stub_list

    total_cells = 1
    for var in all_variables:
        if var == contvariable:
            value_count = len(data_list)
        else:
            value_count = len(values_dict[var])
        constraint_from_top = True
        try:
            value_contsraint = int(self.table_meta_sq[self.table_meta_sq['KEYWORD'] == var]['VALUE'].iloc[0])
            if value_contsraint < 0:
                constraint_from_top = False
                value_contsraint = abs(value_contsraint)
            value_contsraint = min(value_contsraint, value_count) # Reduce to available values
        except IndexError:
            value_contsraint = value_count
        # If number of cells in sq exceeds maximum viewable cells to show in pxWeb2, reduce it with a hard contraint limit:
        if total_cells * value_contsraint > 500000 or value_contsraint == 0:
            value_contsraint = 1
        else:
            total_cells *= value_contsraint
        pxpyfactory.utils.print_filter(f"sq: column {var} has {value_count} values, and it set to show {'first' if constraint_from_top else 'last'} {value_contsraint} values.", 3)

        # Take last N values from values_dict
        if constraint_from_top:
            selected_indices = list(range(0, value_contsraint)) # Make a list of indices to select (first number of values)
        else:
            selected_indices = list(range(value_count - value_contsraint, value_count)) # Make a list of indices to select (last number of values)
        value_codes = [str(i) for i in selected_indices] # Convert the list of numbers to a list of strings
    
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
