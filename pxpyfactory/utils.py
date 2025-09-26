import pandas as pd

# _____________________________________________________________________________
# For each row in metadata_base, if the 'keyword' is found in amendment_df,
# insert the value from amendment_df to the new_column_name-column in base_df.
def metadata_add(base_df, amendment_df, new_column_name):
    base_df[new_column_name] = None
    for idx, row in base_df.iterrows():
        match = amendment_df[amendment_df['KEYWORD'] == row['KEYWORD']]
        if not match.empty:
            base_df.at[idx, new_column_name] = match.iloc[0]['VALUE']
#    print('Added value from other source:')
#    print(base_df[base_df[new_column_name].notnull()])
    return base_df
# _____________________________________________________________________________
# ...
def update_metadata(df, column, updates_dict, mandatory=True, order=500):
    for keyword, new_value in updates_dict.items():
        mask = df['KEYWORD'] == keyword
        print(f"Updating SUBJECT_CODE to: {new_value}") if keyword == 'SUBJECT_CODE' else None
        if mask.sum() == 0: # The keyword does not exist, so we add a new row
            new_row = pd.Series({'KEYWORD': keyword, column: new_value, 'MANDATORY': mandatory, 'TYPE': 'text', 'ORDER': order}).reindex(df.columns)
            df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
        elif mask.sum() == 1: # Only one entry found for the keyword, so we update it
            df.at[df.index[mask][0], column] = new_value
            df.at[df.index[mask][0], 'MANDATORY'] = mandatory
        else: # The keyword exists multiple times -> Raise an error
            raise ValueError(f"Multiple entries found for keyword: {', '.join(df['KEYWORD'].tolist())}")
    # print(f"# Updated metadata for keyword '{keyword}':")
    # print(df[df['KEYWORD'] == keyword])
    return df
# _____________________________________________________________________________
# Prepare the lines that will be written to the .px file
def prepare_px_lines(meta_df, data_lines):
    list_of_lines_to_px = []
    fill_item = "."
    for _, meta_row in meta_df.iterrows():
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
def alert_missing_mandatory(df):
    mandatory_and_missing = df['KEYWORD'][df['MANDATORY'] & (df.apply(lambda row: valid_value(row['VALUE']), axis=1) == False)].tolist()
    try:
        mandatory_and_missing.remove('DATA')
    except ValueError:
        pass  # Do nothing if the string is not in the list
    print(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}") if len(mandatory_and_missing) > 0 else None
    # raise ValueError(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}")

