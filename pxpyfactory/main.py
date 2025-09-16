import pandas as pd
import os
from datetime import datetime
from pxpyfactory.utils import *
from pxpyfactory.file_utils import *
import pprint

def main():
    # _____________________________________________________________________________
    # Set folders and file-paths
    script_path = os.path.dirname(os.path.abspath(__file__)) # Get path for this script location
    input_path = os.path.abspath(os.path.join(script_path, '..', 'input')) # Define input path relative to script location
    common_meta_filepath = os.path.abspath(os.path.join(input_path, 'common_meta.xlsx')) # Define path to common metadata file
    output_path = os.path.abspath(os.path.join(script_path, '..', 'output'))  # Define output path relative to script location
    os.chdir(script_path) # Set working directory

    current_time = datetime.now().strftime("%Y%m%d %H:%M")

    print('=' * 80) # print separator line
    # _____________________________________________________________________________
    # Fetch data products for px file generation
    data_products = file_read(common_meta_filepath, sheet_name='dataprodukter') # Get the overview of all data products
    data_products = data_products.astype(str) # Force content in excel sheet to just strings for easier use
    data_products = data_products[data_products['BUILD_NOW'].isin(['x'])] # Filter to only include tables tagged in Build now column

    duplicates_mask = data_products.duplicated(subset=['TABLE_REF'], keep='first') # List of dublicate table numbers
    duplicates_df = data_products[duplicates_mask].copy() # Dataframe with duplicate table numbers
    data_products = data_products[~duplicates_mask].copy() # Dataframe with unique table numbers (duplicates removed)

    update_folder_structure(data_products, output_path) # Create folder structure from data_products dataframe

    print('Data products / tables to create px-files from:')
    print(data_products)
    if duplicates_df.shape[0] > 0:
        print('--- Duplicated table numbers (will be skipped):')
        print(duplicates_df)
    # _____________________________________________________________________________
    # Prepare general metadata from Excel-sheets - common for all data products
    # Spesific values for each data product will be handled for each data product in separate .csv-files
    meta_default = file_read(common_meta_filepath, sheet_name='metadata-default') # All keywords and agreed setup
    meta_manual = file_read(common_meta_filepath, sheet_name='metadata-manual') # Manual updates on values for some keywords (can be empty) 
    metadata_base = metadata_add(meta_default, meta_manual, 'MANUAL_VALUE') # Adds the column 'manual_value' to 'meta_default'. The value is from 'meta_manual' (match on keyword)
    # _____________________________________________________________________________
    # For each row in the data_products dataframe, process the data product
    # Fetch table_ref column values as a list
    # todo # Remove row and alert if more same table_ref exists on different rows in data_products
    for idx, row in data_products.iterrows():
        table_ref       = row['TABLE_REF']
        table_name      = row['TABLE_NAME_(NO)']
        table_path      = os.path.abspath(os.path.join(input_path, ('TN_' + table_ref + '.csv')))
        table_meta_path = os.path.abspath(os.path.join(input_path, ('TN_' + table_ref + '_meta.csv')))
        px_output_path  = os.path.abspath(os.path.join(output_path, row['LEVEL_1'], row['LEVEL_2'], ('TN_' + table_ref + '.px')))
        subject_code    = row['LEVEL_1'] + '\\' + row['LEVEL_2'] 
        subject_area    = row['LEVEL_1_(NO)'] + '\\' + row['LEVEL_2_(NO)']
        data_list       = [sub.strip().upper() for sub in row['DATA'].split(',')]
        stub_list       = [sub.strip().upper() for sub in row['STUB'].split(',')]
        units_var       = row['UNITS']
        contents_var    = row['CONTENTS']

        print(f"--- Start processing data product / table: {table_ref} {table_name}")
        print(f"data_list: {data_list}")

        table_data = file_read(table_path, sep=',') # Fetch data table from .parquet or .csv file

        # Ensure all non-data columns are strings
        for column in table_data.columns:
            if column not in data_list:
                table_data[column] = table_data[column].apply(lambda x: str(x) if pd.notnull(x) else x)

        column_list = list(table_data.columns) # Fetch column list from data table
        for data_col in data_list:
            column_list.remove(data_col) # Remove data column from the list of columns to process
        heading_list = [column for column in column_list if column not in stub_list] # Headings are all columns not in stub_list

        # Create a dictionary with unique values for each column in the data table
        values_dict = {}
        for column in column_list:
            values_dict[column] = list(set(table_data[column]))
        # pprint.pp(values_dict)

        # Calculate data matrix dimensions and reshape data to fit PX format
        data_out = prepare_data_matrix(values_dict, stub_list, heading_list, data_list, table_data)

        # Adds the column 'spesific_value' to 'metadata'. The value is from 'table_meta' (match on keyword)
        # This is spesific metadata for this data product - stored in a separate .csv-file
        table_meta = file_read(table_meta_path)
        if table_meta.empty: # If the spesific metadata file is missing or empty, create an empty dataframe
            table_meta = pd.DataFrame(columns=['KEYWORD', 'VALUE'])
        metadata = metadata_add(metadata_base.copy(), table_meta, 'SPESIFIC_VALUE') # Merge the two sources of metadata
        metadata['MANDATORY'] = metadata['MANDATORY'].fillna('').str.lower().isin(['yes', 'nav']).astype(bool) # Mandatory column cleanup 

        metadata = update_metadata(metadata, 'MATRIX', 'MANUAL_VALUE', 'TN_' + table_ref)
        metadata = update_metadata(metadata, 'TITLE', 'MANUAL_VALUE', table_name)
        metadata = update_metadata(metadata, 'STUB', 'MANUAL_VALUE', stub_list)
        metadata = update_metadata(metadata, 'HEADING', 'MANUAL_VALUE', heading_list)
        # metadata = update_metadata(metadata, 'CONTVARIABLE', 'MANUAL_VALUE', data_column)
        metadata = update_metadata(metadata, 'UNITS', 'MANUAL_VALUE', units_var)
        metadata = update_metadata(metadata, 'SUBJECT-CODE', 'MANUAL_VALUE', subject_code)
        metadata = update_metadata(metadata, 'SUBJECT-AREA', 'MANUAL_VALUE', subject_area)
        metadata = update_metadata(metadata, 'CONTENTS', 'MANUAL_VALUE', contents_var)
        metadata = update_metadata(metadata, 'LAST-UPDATED', 'MANUAL_VALUE', current_time)

        for column_name in column_list:
            # print(f"Kolonne med {len(values_dict[column_name])} unike verdier: {column_name} - {str(values_dict[column_name])}")
            metadata = update_metadata(metadata, 'VALUES("' + column_name + '")', 'MANUAL_VALUE', values_dict[column_name])

        # Set the value to the first non-null value in this priority
        metadata['VALUE'] = metadata[['SPESIFIC_VALUE', 'MANUAL_VALUE', 'DEFAULT_VALUE']].apply(get_first_notnull, axis=1)

        # pd.set_option('display.max_rows', None)
        # print(metadata[['KEYWORD', 'VALUE', 'SPESIFIC_VALUE', 'MANUAL_VALUE', 'DEFAULT_VALUE']])

        meta_out = metadata.copy()[(metadata['MANDATORY']) | (metadata.apply(lambda row: valid_value(row['VALUE']), axis=1))][['ORDER', 'KEYWORD', 'VALUE', 'TYPE', 'MANDATORY']].sort_values('ORDER')
        alert_missing_mandatory(meta_out) # Alert if any mandatory keywords are missing values
        list_of_lines = prepare_px_lines(meta_out, data_out) # Just print it out for now
        write_px(list_of_lines, file_path=px_output_path)

