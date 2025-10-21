import pandas as pd
from pxpyfactory.io_utils import file_read

# _____________________________________________________________________________
def prepare_data_products(common_meta_filepath):
    data_products = file_read(common_meta_filepath, sheet_name='dataprodukter') # Get the overview of all data products
    data_products = data_products.applymap(lambda x: str(x) if pd.notnull(x) else None) # Force content in excel sheet to just strings for easier use (None for empty cells)
    # Column referances in the data_products sheet
    data_products.rename(columns={'BYGG_NAA'      : 'BUILD_NOW'      }, inplace=True)
    data_products.rename(columns={'OMRAADE_MAPPE' : 'LEVEL_1_FOLDER' }, inplace=True)
    data_products.rename(columns={'OMRAADE'       : 'LEVEL_1'        }, inplace=True) # område oversettes noen ganger i Nav til field
    data_products.rename(columns={'TEMA_MAPPE'    : 'LEVEL_2_FOLDER' }, inplace=True)
    data_products.rename(columns={'TEMA'          : 'LEVEL_2'        }, inplace=True) # tema oversettes ofte i Nav til theme
    data_products.rename(columns={'NUMMER'        : 'TABLE_NO'       }, inplace=True)
    data_products.rename(columns={'TITTEL'        : 'TITLE'          }, inplace=True)
    data_products.rename(columns={'BESKRIVELSE'   : 'CONTENTS'       }, inplace=True)
    # data_products.rename(columns={'STUB'          : 'STUB'           }, inplace=True)
    # data_products.rename(columns={'HEADING'       : 'HEADING'        }, inplace=True)
    # data_products.rename(columns={'DATA'          : 'DATA'           }, inplace=True)
    # data_products.rename(columns={'UNITS'         : 'UNITS'          }, inplace=True)
    # data_products.rename(columns={'SEP'           : 'SEP'            }, inplace=True)

    data_products = data_products[data_products['BUILD_NOW'] == 'x'] # .isin(['x'])] # Filter to only include tables tagged in Build now column

    duplicates_mask = data_products.duplicated(subset=['TABLE_NO'], keep='first') # List of dublicate table numbers
    duplicates_df = data_products[duplicates_mask].copy() # Dataframe with duplicate table numbers
    data_products = data_products[~duplicates_mask].copy() # Dataframe with unique table numbers (duplicates removed)

    print('\nData products / tables to create px-files from:')
    print(data_products[['LEVEL_1_FOLDER', 'LEVEL_2_FOLDER', 'TABLE_NO', 'TITLE', 'STUB', 'HEADING', 'DATA', 'UNITS']])
    if duplicates_df.shape[0] > 0:
        print('--- Duplicated table numbers (will be skipped):')
        print(duplicates_df)

    return data_products

# _____________________________________________________________________________
def prepare_metadata_base(common_meta_filepath):
    # Prepare general metadata from Excel-sheets - common for all data products
    # Spesific values for each data product will be handled for each data product in separate .csv-files
    meta_default = file_read(common_meta_filepath, sheet_name='metadata-default')
    meta_default = meta_default[['ORDER','KEYWORD','MANDATORY','DEFAULT_VALUE','TYPE']] # Keep only relevant columns
    meta_manual = file_read(common_meta_filepath, sheet_name='metadata-manual') # Manual updates on values for some keywords (can be empty) 
    metadata_base = metadata_add(meta_default, meta_manual, 'MANUAL_VALUE') # Adds the column 'manual_value' to 'meta_default'. The value is from 'meta_manual' (match on keyword)

    return metadata_base
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
            new_row = pd.Series({'KEYWORD': keyword, column: new_value, 'MANDATORY': mandatory, 'TYPE': 'text', 'ORDER': order}).reindex(metadata.columns)
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
            # for i, row in enumerate(data_df.itertuples(index=False, name=None)):
                value_out += '\n' + line
                # formatted_row = [f'"{fill_item}"' if pd.isnull(item) else item for item in row]
                # value_out += ' '.join(map(str, formatted_row))
                # value_out += ' '.join(map(str, line))
        # If none of the above, assume text
        elif isinstance(row_value, str):
            # print(f"Preparing for .px: {row_keyword}={row_value}, type desc={row_type}, type of current value={type(row_value)})")
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
        # print(f"Prepared for .px: {row_keyword}={value_out}, mandatory={meta_row['MANDATORY']}") if row_keyword != 'DATA' else None
        list_of_lines_to_px.append(f'{row_keyword}={value_out};')
    return list_of_lines_to_px

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
def prep_list_from_string(in_string, separator=',', to_upper=True):
    if in_string is None:
        out_list = []
    elif isinstance(in_string, str):
        if to_upper:
            out_list = [sub.strip().upper() for sub in in_string.split(separator)]
        else:
            out_list = [sub.strip() for sub in in_string.split(separator)]
    elif isinstance(in_string, (int, float)):
        out_list = [str(in_string)]
    else:
        out_list = []
    return out_list
# _____________________________________________________________________________
def valid_value(value):
    # If value is a list, tuple, or np.ndarray
    if isinstance(value, (list, tuple, pd.Series)): #, np.ndarray)):
        # If it has exactly one element and that element is null, skip it
        if len(value) == 0 or (len(value) == 1 and pd.isnull(value[0])):
            return False
        else:
            return True
    # If value is a scalar and not null, return it
    elif value == '':
        return False
    elif pd.notnull(value):
        return True
    else:
        return False
# _____________________________________________________________________________
def alert_missing_mandatory(metadata):
    mandatory_and_missing = metadata['KEYWORD'][metadata['MANDATORY'] & (metadata.apply(lambda row: valid_value(row['VALUE']), axis=1) == False)].tolist()
    try:
        mandatory_and_missing.remove('DATA')
    except ValueError:
        pass  # Do nothing if the string is not in the list
    print(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}") if len(mandatory_and_missing) > 0 else None
    # raise ValueError(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}")

