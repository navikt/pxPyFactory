import pandas as pd
import os
from itertools import product
from datetime import datetime
from pxpyfactory.utils import *
from pxpyfactory.file_utils import *
# import pprint

def process_data_product(row, metadata_base, input_path, output_path):
    metadata = metadata_base.copy() # Start with a fresh copy of the base metadata for each data product
    table_ref       = 'NAV_' + row['TABLE_NO'] # Nav uses NAV_ as a prefix for table numbers
    table_name      = row['TITLE']
    table_sep       = row['SEP'] # Separator used in .csv-file (used for reading input file)
    subject_code    = row['LEVEL_1_FOLDER'] # + '\\' + row['LEVEL_2_FOLDER'] 
    subject_area    = row['LEVEL_1'] # + '\\' + row['LEVEL_2']
    data_list       = [sub.strip().upper() for sub in row['DATA'].split(',')]
    stub_list       = [sub.strip().upper() for sub in row['STUB'].split(',')]
    units_list      = [sub.strip().upper() for sub in row['UNITS'].split(',')]
    # units_var       = row['UNITS']
    contents_var    = row['CONTENTS']
    contvariable    = 'stat_var' # data_list[0] # Column name for contents variable - can probably be anything..

    table_path      = os.path.abspath(os.path.join(input_path, (table_ref + '.csv')))
    table_meta_path = os.path.abspath(os.path.join(input_path, (table_ref + '_meta.csv')))
    px_output_path  = os.path.abspath(os.path.join(output_path, row['LEVEL_1_FOLDER'], row['LEVEL_2_FOLDER'], (table_ref + '.px')))

    print(f"\n{table_ref} {table_name} - Start processing data product / table")

    table_data = file_read(table_path, sep=table_sep) # Fetch data table from .parquet or .csv file
    print(f"  Columns: {list(table_data.columns)}")

    # Ensure all non-data columns are strings
    for column in table_data.columns:
        if column not in data_list:
            table_data[column] = table_data[column].apply(lambda x: str(x) if pd.notnull(x) else x)

    column_list = list(table_data.columns) # Fetch column list from data table
    for data_col in data_list:
        try:
            column_list.remove(data_col) # Remove data column from the list of columns to process
        except ValueError:
            print(f"WARNING: Data column {data_col} not found in data table for {table_ref}.")
    heading_list = [contvariable]
    heading_list += [column for column in column_list if column not in stub_list] # Headings are all columns not in stub_list

    # -----------------------------------------------------------------
    # Prepare dictionary with px-parameters from input files:
    manual_metadata_updates_dict = {
        'TABLEID':      table_ref     ,
        'MATRIX':       table_ref     ,
        'TITLE':        table_name    ,
        'STUB':         stub_list     ,
        'HEADING':      heading_list  ,
        'CONTVARIABLE': contvariable  ,
        'UNITS':        '-'           , # Due to a faulty need for a plain UNITS, only the first unit is used here (unts for the rest are added later)
        'SUBJECT-CODE': subject_code  ,
        'SUBJECT-AREA': subject_area  ,
        'CONTENTS':     contents_var  ,
    }
    # Add px-parameter for each data column:
    for index, data_col in enumerate(data_list):
        manual_metadata_updates_dict['UNITS("' + data_col + '")'] = units_list[index] if index < len(units_list) else units_list[0] # If there is more data columns than units, use the first unit for the rest
        manual_metadata_updates_dict['LAST-UPDATED("' + data_col + '")'] = datetime.now().strftime("%Y%m%d %H:%M") # current time on px-format
        # If needed, more data-column specific px-parameters can be added. Value can be based on 'spesific_value' and implementet a few lines lower.
        # manual_metadata_updates_dict['STOCKFA("' + data_col + '")'] = 
        # manual_metadata_updates_dict['CFPRICES("' + data_col + '")'] = 
        # manual_metadata_updates_dict['DAYADJ("' + data_col + '")'] = 
        # manual_metadata_updates_dict['SEASADJ("' + data_col + '")'] = 

    values_dict = {} # Dictionary that will contain the unique values for each column in the data table
    for column_name in column_list:
        values_dict[column_name] = sorted(pd.unique(table_data[column_name]))
        manual_metadata_updates_dict['VALUES("' + column_name + '")'] = values_dict[column_name]

    manual_metadata_updates_dict['VALUES("' + contvariable + '")'] = data_list

    # Update metadata with the px-parameters from the dictionary above:
    metadata = update_metadata(metadata, 'MANUAL_VALUE', manual_metadata_updates_dict)

    # -----------------------------------------------------------------
    # Add the column 'spesific_value' to 'metadata'. The value is from 'table_meta' (match on keyword).
    # This is spesific metadata for this data product - stored in a separate .csv-file
    table_meta = file_read(table_meta_path, sep=table_sep)
    if {'KEYWORD', 'VALUE'}.issubset(table_meta.columns): # Use spesific metadata if itis valid
        print("File for spesific metadata found.")
    else:
        table_meta = pd.DataFrame(columns=['KEYWORD', 'VALUE'])
    metadata = metadata_add(metadata, table_meta, 'SPESIFIC_VALUE') # Merge the two sources of metadata
    metadata['MANDATORY'] = metadata['MANDATORY'].fillna('').str.lower().isin(['yes', 'nav']).astype(bool) # Mandatory column cleanup 

    # -----------------------------------------------------------------
    # Set the value to the first non-null value in this priority:
    metadata['VALUE'] = metadata[['SPESIFIC_VALUE', 'MANUAL_VALUE', 'DEFAULT_VALUE']].apply(get_first_notnull, axis=1)
    metadata = metadata[['ORDER', 'KEYWORD', 'VALUE', 'TYPE', 'MANDATORY']] # Keep only relevant columns
    # Filter out rows with no value, unless they are mandatory:
    metadata = metadata[(metadata['MANDATORY']) | (metadata.apply(lambda row: valid_value(row['VALUE']), axis=1))].sort_values('ORDER')

    stub_columns = [s.upper() for s in stub_list]
    data_columns = [s.upper() for s in data_list]
    piv_columns = column_list.copy()
    for stub_col in stub_columns:
        piv_columns.remove(stub_col)


    # Create a DataFrame with all possible combinations:
    all_combinations = pd.DataFrame(list(product(*values_dict.values())), columns=stub_columns + piv_columns)
    # Merge full matrix with data, and fill missing cells:
    fill_value = metadata.loc[metadata['KEYWORD'] == 'DATASYMBOL2', 'VALUE'].iloc[0]
    expanded_table_data = pd.merge(all_combinations, table_data, on=stub_columns + piv_columns, how='left').fillna(fill_value)
    # Pivot the table to get the desired format:
    data_pivot = expanded_table_data.pivot_table(index=stub_columns, columns=piv_columns, values=data_columns, aggfunc='first')
    data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string

    # -----------------------------------------------------------------
    alert_missing_mandatory(metadata) # Alert if any mandatory keywords are missing values
    list_of_lines = prepare_px_lines(metadata, data_lines)

    return list_of_lines, px_output_path


