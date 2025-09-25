import pandas as pd
import os
from itertools import product
from datetime import datetime
from pxpyfactory.utils import *
from pxpyfactory.file_utils import *
import pprint

def main():
    print('=' * 80) # print separator line
    # _____________________________________________________________________________
    # Set folders and file-paths
    script_path = os.path.dirname(os.path.abspath(__file__)) # Get path for this script location
    input_path = os.path.abspath(os.path.join(script_path, '..', 'input')) # Define input path relative to script location
    common_meta_filepath = os.path.abspath(os.path.join(input_path, 'common_meta.xlsx')) # Define path to common metadata file
    output_path = os.path.abspath(os.path.join(script_path, '..', 'output'))  # Define output path relative to script location
    os.chdir(script_path) # Set working directory

    current_time = datetime.now().strftime("%Y%m%d %H:%M")

    # _____________________________________________________________________________
    # Fetch data products for px file generation
    data_products = file_read(common_meta_filepath, sheet_name='dataprodukter') # Get the overview of all data products
    data_products = data_products.astype(str) # Force content in excel sheet to just strings for easier use
    # Column referances in the data_products sheet
    data_products.rename(columns={'BYGG_NAA': 'BUILD_NOW'}, inplace=True)
    data_products.rename(columns={'OMRAADE_MAPPE': 'LEVEL_1_FOLDER'}, inplace=True)
    data_products.rename(columns={'OMRAADE': 'LEVEL_1'}, inplace=True) # område oversettes noen ganger i Nav til field
    data_products.rename(columns={'TEMA_MAPPE': 'LEVEL_2_FOLDER'}, inplace=True)
    data_products.rename(columns={'TEMA': 'LEVEL_2'}, inplace=True) # tema oversettes ofte i Nav til theme
    data_products.rename(columns={'NUMMER': 'TABLE_NO'}, inplace=True)
    data_products.rename(columns={'TITTEL': 'TITLE'}, inplace=True)
    data_products.rename(columns={'BESKRIVELSE': 'CONTENTS'}, inplace=True)
    # data_products.rename(columns={'STUB': 'STUB'}, inplace=True)
    # data_products.rename(columns={'DATA': 'DATA'}, inplace=True)
    # data_products.rename(columns={'UNITS': 'UNITS'}, inplace=True)
    data_products = data_products[data_products['BUILD_NOW'].isin(['x'])] # Filter to only include tables tagged in Build now column
    duplicates_mask = data_products.duplicated(subset=['TABLE_NO'], keep='first') # List of dublicate table numbers
    duplicates_df = data_products[duplicates_mask].copy() # Dataframe with duplicate table numbers
    data_products = data_products[~duplicates_mask].copy() # Dataframe with unique table numbers (duplicates removed)

    update_folder_structure(data_products, output_path) # Create folder structure from data_products dataframe

    print('\nData products / tables to create px-files from:')
    print(data_products[['LEVEL_1_FOLDER', 'LEVEL_2_FOLDER', 'TABLE_NO', 'TITLE', 'STUB', 'DATA', 'UNITS']])
    if duplicates_df.shape[0] > 0:
        print('--- Duplicated table numbers (will be skipped):')
        print(duplicates_df)
    # _____________________________________________________________________________
    # Prepare general metadata from Excel-sheets - common for all data products
    # Spesific values for each data product will be handled for each data product in separate .csv-files
    meta_default = file_read(common_meta_filepath, sheet_name='metadata-default')
    meta_default = meta_default[['ORDER','KEYWORD','MANDATORY','DEFAULT_VALUE','TYPE']] # Keep only relevant columns
    meta_manual = file_read(common_meta_filepath, sheet_name='metadata-manual') # Manual updates on values for some keywords (can be empty) 
    metadata_base = metadata_add(meta_default, meta_manual, 'MANUAL_VALUE') # Adds the column 'manual_value' to 'meta_default'. The value is from 'meta_manual' (match on keyword)
    # _____________________________________________________________________________
    # For each row in the data_products dataframe, process the data product
    # Fetch table_ref column values as a list
    # todo # Remove row and alert if more same table_ref exists on different rows in data_products
    for idx, row in data_products.iterrows():
        table_ref       = 'NAV_' + row['TABLE_NO'] # Nav uses NAV_ as a prefix for table numbers
        table_name      = row['TITLE']
        table_sep       = row['SEP'] # Separator used in .csv-file (used for reading input file)
        table_path      = os.path.abspath(os.path.join(input_path, (table_ref + '.csv')))
        table_meta_path = os.path.abspath(os.path.join(input_path, (table_ref + '_meta.csv')))
        px_output_path  = os.path.abspath(os.path.join(output_path, row['LEVEL_1_FOLDER'], row['LEVEL_2_FOLDER'], (table_ref + '.px')))
        subject_code    = row['LEVEL_1_FOLDER'] # + '\\' + row['LEVEL_2_FOLDER'] 
        subject_area    = row['LEVEL_1'] # + '\\' + row['LEVEL_2']
        data_list       = [sub.strip().upper() for sub in row['DATA'].split(',')]
        stub_list       = [sub.strip().upper() for sub in row['STUB'].split(',')]
        units_list      = [sub.strip().upper() for sub in row['UNITS'].split(',')]
        # units_var       = row['UNITS']
        contents_var    = row['CONTENTS']
        contvariable    = 'stat_var' # data_list[0] # Column name for contents variable - can probably be anything..


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

        # Adds the column 'spesific_value' to 'metadata'. The value is from 'table_meta' (match on keyword)
        # This is spesific metadata for this data product - stored in a separate .csv-file
        table_meta = file_read(table_meta_path, sep=table_sep)
        if {'KEYWORD', 'VALUE'}.issubset(table_meta.columns): # Use spesific metadata if itis valid
            print("File for spesific metadata found.")
        else:
            table_meta = pd.DataFrame(columns=['KEYWORD', 'VALUE'])
        metadata = metadata_add(metadata_base.copy(), table_meta, 'SPESIFIC_VALUE') # Merge the two sources of metadata
        metadata['MANDATORY'] = metadata['MANDATORY'].fillna('').str.lower().isin(['yes', 'nav']).astype(bool) # Mandatory column cleanup 

#----------------------------------
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

        # Add metadata for each data column
        for index, data_col in enumerate(data_list):
            manual_metadata_updates_dict['LAST-UPDATED("' + data_col + '")'] = current_time
            manual_metadata_updates_dict['UNITS("' + data_col + '")'] = units_list[index] if index < len(units_list) else units_list[0] # If there is more data columns than units, use the first unit for the rest
        
        values_dict = {} # Dictionary that will contain the unique values for each column in the data table
        for column_name in column_list:
            values_dict[column_name] = sorted(pd.unique(table_data[column_name]))
            # print(f"Kolonne med {len(values_dict[column_name])} unike verdier: {column_name} - {str(values_dict[column_name])}")
            manual_metadata_updates_dict['VALUES("' + column_name + '")'] = values_dict[column_name]
        manual_metadata_updates_dict['VALUES("' + contvariable + '")'] = data_list

        metadata = update_metadata(metadata, 'MANUAL_VALUE', manual_metadata_updates_dict)

        # Set the value to the first non-null value in this priority
        metadata['VALUE'] = metadata[['SPESIFIC_VALUE', 'MANUAL_VALUE', 'DEFAULT_VALUE']].apply(get_first_notnull, axis=1)
        metadata = metadata[['ORDER', 'KEYWORD', 'VALUE', 'TYPE', 'MANDATORY']] # Keep only relevant columns
        # Filter out rows with no value, unless they are mandatory:
        metadata = metadata[(metadata['MANDATORY']) | (metadata.apply(lambda row: valid_value(row['VALUE']), axis=1))].sort_values('ORDER')

        piv_columns = column_list.copy()
        for stub_col in stub_list:
            piv_columns.remove(stub_col) # Remove stub column from the list of columns to process

        # Create a DataFrame with all possible combinations:
        all_combinations = pd.DataFrame(list(product(*values_dict.values())), columns=stub_list + piv_columns)
        # Merge with your original data, and fill missing combinations:
        fill_value = metadata.loc[metadata['KEYWORD'] == 'DATASYMBOL2', 'VALUE'].iloc[0]
        expanded_table_data = pd.merge(all_combinations, table_data, on=stub_list + piv_columns, how='left').fillna(fill_value)
        data_pivot = expanded_table_data.pivot_table(index=stub_list, columns=piv_columns, values=data_list, aggfunc='first') # Pivot the table to get the desired format
        data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string

        alert_missing_mandatory(metadata) # Alert if any mandatory keywords are missing values
        list_of_lines = prepare_px_lines(metadata, data_lines)
        write_px(list_of_lines, file_path=px_output_path)

    print('')

