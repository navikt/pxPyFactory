import sys
import pandas as pd
import re
from datetime import datetime
import pxpyfactory.io_utils

# _____________________________________________________________________________
def prepare_data_products(common_meta_filepath):
    data_products = pxpyfactory.io_utils.file_read(common_meta_filepath, sheet_name='dataprodukter') # Get the overview of all data products
    data_products = data_products.map(lambda x: str(x) if pd.notnull(x) else None) # Force content in excel sheet to just strings for easier use (None for empty cells)
    # Column referances in the data_products sheet
    data_products.rename(columns={'BYGG_NAA'      : 'BUILD_NOW'      }, inplace=True)
    data_products.rename(columns={'OMRAADE'       : 'LEVEL_1'        }, inplace=True) # område oversettes noen ganger i Nav til field
    data_products.rename(columns={'TEMA'          : 'LEVEL_2'        }, inplace=True) # tema oversettes ofte i Nav til theme
    data_products.rename(columns={'NUMMER'        : 'TABLE_REF'      }, inplace=True)
    data_products.rename(columns={'TITTEL'        : 'TITLE'          }, inplace=True)
    data_products.rename(columns={'BESKRIVELSE'   : 'CONTENTS'       }, inplace=True)
    # data_products.rename(columns={'STUB'          : 'STUB'           }, inplace=True)
    # data_products.rename(columns={'HEADING'       : 'HEADING'        }, inplace=True)
    # data_products.rename(columns={'DATA'          : 'DATA'           }, inplace=True)
    # data_products.rename(columns={'UNITS'         : 'UNITS'          }, inplace=True)
    # data_products.rename(columns={'SEP'           : 'SEP'            }, inplace=True)

    data_products['TABLE_REF_RAW'] = data_products['TABLE_REF']
    data_products['TABLE_REF'] = data_products['TABLE_REF'].apply(_shorten_table_ref)

    # Filter based on command line arguments
    input_arg = sys.argv[1] if len(sys.argv) > 1 else None
    print(f"Args: {input_arg}")
    if input_arg == None: # No arguments given - build only tables where input has changed
        data_products['FORCE_BUILD'] = None
    elif input_arg == 'all': # Build all tables, even if input has not changed
        data_products['FORCE_BUILD'] = True
    else: # Build only table specified
        data_products['FORCE_BUILD'] = False
        data_products.loc[data_products['TABLE_REF'] == input_arg, 'FORCE_BUILD'] = True
        data_products.loc[data_products['TABLE_REF_RAW'] == input_arg, 'FORCE_BUILD'] = True

    # Remove data products where BUILD_NOW is not set to 'x' in Excel sheet and FORCE_BUILD is not True or None
    data_products = data_products[(data_products['BUILD_NOW'] == 'x') & (data_products['FORCE_BUILD'] != False)]

    duplicates_mask = data_products.duplicated(subset=['TABLE_REF'], keep='first') # List of dublicate table numbers
    duplicates_df = data_products[duplicates_mask].copy() # Dataframe with duplicate table numbers
    data_products = data_products[~duplicates_mask].copy() # Dataframe with unique table numbers (duplicates removed)

    print_filter('Data products / tables to create px-files from:', 0)
    print_filter(data_products[['LEVEL_1', 'LEVEL_2', 'TABLE_REF', 'TITLE', 'STUB', 'HEADING', 'DATA', 'UNITS']], 0)
    if duplicates_df.shape[0] > 0:
        print_filter('--- Duplicated table numbers (will be skipped):', 0)
        print_filter(duplicates_df, 0)

    return data_products
# _____________________________________________________________________________
# Shorten table reference to max 20 chars (excluding separators) by truncating each part
def _shorten_table_ref(table_ref):
    table_ref_str = str(table_ref)
    text_parts = re.split(r'[_-]', table_ref_str) # Split by '_' and '-' to get text parts only
    
    # Check if total length (without separators) exceeds 20
    total_length = sum(len(p) for p in text_parts)
    if total_length <= 20:
        # Still remove separators even if not too long
        return ''.join(text_parts)
    
    max_chars = 20 // len(text_parts)  # Calculate max chars per part by floor division
    truncated_parts = [p[:max_chars] for p in text_parts] # Truncate each text part and join without separators
    
    return ''.join(truncated_parts)

# _____________________________________________________________________________
def prepare_metadata_base(common_meta_filepath):
    # Prepare general metadata from Excel-sheets - common for all data products
    # Spesific values for each data product will be handled for each data product in separate .csv-files
    meta_default = pxpyfactory.io_utils.file_read(common_meta_filepath, sheet_name='metadata-default')
    meta_default = meta_default[['ORDER','KEYWORD','MANDATORY','DEFAULT_VALUE','TYPE']] # Keep only relevant columns
    meta_manual = pxpyfactory.io_utils.file_read(common_meta_filepath, sheet_name='metadata-manual') # Manual updates on values for some keywords (can be empty) 
    metadata_base = metadata_add(meta_default, meta_manual, 'MANUAL_VALUE') # Adds the column 'manual_value' to 'meta_default'. The value is from 'meta_manual' (match on keyword)

    return metadata_base
# _____________________________________________________________________________
def prepare_alias(common_meta_filepath):
    # Prepare alias folder names from Excel-sheets - common for all data products
    alias = pxpyfactory.io_utils.file_read(common_meta_filepath, sheet_name='folder-alias')
    alias = alias[['CODE','NO','EN']] # Keep only relevant columns
    alias['NO'] = alias['NO'].where(alias['NO'].apply(valid_value), alias['EN']) # Copy EN to NO where NO is invalid
    alias['EN'] = alias['EN'].where(alias['EN'].apply(valid_value), alias['NO']) # Copy NO to EN where EN is invalid
    alias = alias[alias['CODE'].apply(valid_value) & alias['NO'].apply(valid_value)] # Keep only rows where both CODE and NO are valid

    duplicates_mask = alias.duplicated(subset=['CODE'], keep='first') # List of dublicate table numbers
    # duplicates_df = alias[duplicates_mask].copy() # Dataframe with duplicate table numbers
    alias = alias[~duplicates_mask].copy() # Dataframe with unique table numbers (duplicates removed)
    return alias
# _____________________________________________________________________________
# Create folder structure from data_products dataframe
# Create the folder structure to put the px-files in
def update_folder_structure(data_products_df, alias_df, output_path):
    path_list = []
    languages = ['no', 'en']
    for _, row in data_products_df.iterrows():
        level1_path = output_path + '/' + str(row['LEVEL_1']).replace('/', '')
        level2_path = level1_path + '/' + str(row['LEVEL_2']).replace('/', '')

        if level1_path not in path_list:
            path_list.append(level1_path)
        if level2_path not in path_list:
            path_list.append(level2_path)

    for path in path_list:
        leaf = path.rsplit('/', 1)[-1]
        for language in languages:
            alias_value = leaf
            if leaf in alias_df['CODE'].values:
                alias_value = alias_df.loc[alias_df['CODE'] == leaf, language.upper()].iloc[0]
            file_path = path + '/' + 'alias_' + language + '.txt'
            pxpyfactory.io_utils.file_write(file_path, alias_value)
# _____________________________________________________________________________
# For each row in metadata_base, if the 'keyword' is found in amendment_df,
# insert the value from amendment_df to the new_column_name-column in metadata.
def metadata_add(metadata, amendment_df, new_column_name):
    metadata[new_column_name] = None
    for idx, row in metadata.iterrows():
        match = amendment_df[amendment_df['KEYWORD'] == row['KEYWORD']]
        if not match.empty:
            metadata.at[idx, new_column_name] = match.iloc[0]['VALUE']
#    print('Added value from other source:')
#    print(metadata[metadata[new_column_name].notnull()])
    return metadata
# _____________________________________________________________________________
# ...
def update_metadata(metadata, column, updates_dict, mandatory=True, order=500):
    for keyword, new_value in updates_dict.items():
        mask = metadata['KEYWORD'] == keyword
        print(f"Updating SUBJECT_CODE to: {new_value}") if keyword == 'SUBJECT_CODE' else None
        if mask.sum() == 0: # The keyword does not exist, so we add a new row
            value_type = 'text'
            if isinstance(new_value, int):
                value_type = 'integer'
            new_row = pd.Series({'KEYWORD': keyword, column: new_value, 'MANDATORY': mandatory, 'TYPE': value_type, 'ORDER': order}).reindex(metadata.columns)
            metadata = pd.concat([metadata, new_row.to_frame().T], ignore_index=True)
        elif mask.sum() == 1: # Only one entry found for the keyword, so we update it
            metadata.at[metadata.index[mask][0], column] = new_value
            metadata.at[metadata.index[mask][0], 'MANDATORY'] = mandatory
        else: # The keyword exists multiple times -> Raise an error
            raise ValueError(f"Multiple entries found for keyword: {', '.join(metadata['KEYWORD'].tolist())}")
    # print(f"# Updated metadata for keyword '{keyword}':")
    # print(metadata[metadata['KEYWORD'] == keyword])
    return metadata
# _____________________________________________________________________________
# Prepare the lines that will be written to the .px file
def serialize_to_px_format(metadata, data_lines):
    list_of_lines_to_px = []
    fill_item = "."
    for _, meta_row in metadata.iterrows():
        row_keyword = meta_row['KEYWORD']
        row_value = meta_row['VALUE']
        row_type = meta_row['TYPE']
        if row_type == 'integer':
            value_out = row_value
        elif row_type == 'data': # This is only for the DATA part
            value_out = ''
            for line in data_lines:
                value_out += '\n' + line
        # If none of the above, assume text
        elif isinstance(row_value, str):
            value_out = '"' + row_value + '"'
        elif isinstance(row_value, list):
            if all(isinstance(x, str) for x in row_value):
                value_out = '"' + '", "'.join(row_value) + '"'
            elif any(isinstance(x, str) for x in row_value):
                value_out = '"' + '", "'.join([str(x) for x in row_value]) + '"'
            else: # all(isinstance(x, (int, float)) for x in row_value):
                value_out = ', '.join(str(x) for x in row_value)
        elif row_type == 'text':
            value_out = '"' + str(row_value) + '"'
        else:
            print(f"Unclear type for: keyword={row_keyword}, value={row_value}, type desc={row_type}, type of current value={type(row_value)})")
            value_out = '"' + str(row_value) + '"'
        list_of_lines_to_px.append(f'{row_keyword}={value_out};')
    return list_of_lines_to_px

# _____________________________________________________________________________
def prep_list_from_string(in_string, separator=',', to_upper=True, split_part=0):
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
    
# _____________________________________________________________________________
def alert_missing_mandatory(metadata):
    mandatory_and_missing = metadata['KEYWORD'][metadata['MANDATORY'] & (metadata.apply(lambda row: valid_value(row['VALUE']), axis=1) == False)].tolist()
    try:
        mandatory_and_missing.remove('DATA')
    except ValueError:
        pass  # Do nothing if the string is not in the list
    print(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}") if len(mandatory_and_missing) > 0 else None
    # raise ValueError(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}")
# _____________________________________________________________________________
def valid_value_or_none(value, full=False):
    # If value is a list, tuple, or np.ndarray
    if isinstance(value, (list, tuple, pd.Series)): #, np.ndarray)):
        # If it has exactly one element and that element is null, skip it
        if len(value) == 0 or (len(value) == 1 and pd.isnull(value[0])):
            return None
        else:
            return value
    elif isinstance(value, datetime):
        # Remove timezone info to allow comparison with naive datetimes
        if value.tzinfo is not None:
            value = value.replace(tzinfo=None)
        # print(f"Datetime value detected: {value}")
        time_from = datetime(2020, 1, 1) # earliest valid time
        time_to = datetime(2099, 12, 31) # latest valid time
        if time_from <= value <= time_to:
            return get_time_formatted(value)
    elif value == '':
        return None
    elif isinstance(value, str) and value in ['-','.','..'] and full:
        return None
    elif isinstance(value, str) and value.lower() in ['none','null','nan','nat']:
        return None
    elif pd.notnull(value):
        return value
    else:
        return None
# _____________________________________________________________________________
def valid_value(value):
    return not valid_value_or_none(value) is None
# _____________________________________________________________________________
def same_value(value1, value2):
    return valid_value_or_none(value1, full=True) == valid_value_or_none(value2, full=True)
# _____________________________________________________________________________
def get_time_formatted(timestamp=None):
    if timestamp is None:
        return_time = datetime.now()
    elif isinstance(timestamp, datetime):
        return_time = timestamp
    else:
        return_time = datetime.fromtimestamp(float(timestamp))
    return return_time.strftime("%Y%m%d %H:%M")
    # if seconds: return return_time.strftime("%Y-%m-%d %H:%M:%S")
# _____________________________________________________________________________
# Returns the first non-null value in a row. If the value is a list or tuple,
# it checks if it has exactly one element and that element is null, in which case it skips it.
# If all values are null, it returns an empty string.
def get_first_notnull(row):
    for value in row:
        if valid_value(value):
            return value
        else:
            continue
    return ''
# _____________________________________________________________________________
def is_list_empty(check_list):
    if check_list is None:
        return True
    elif check_list in ([''], ['NAN']):
        return True
    elif len(check_list) == 0:
        return True
    else:
        return False
# _____________________________________________________________________________
def print_filter(output, priority_level=0):
    if priority_level <= 1:
        print(output)
