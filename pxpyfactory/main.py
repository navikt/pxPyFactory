import pandas as pd
import os
from pxpyfactory.main_comp import *
from pxpyfactory.utils import *
from pxpyfactory.file_utils import *

def main():
    print('=' * 80) # print separator line
    # Set folders and file-paths
    script_path = os.path.dirname(os.path.abspath(__file__)) # Get path for this script location
    input_path = os.path.abspath(os.path.join(script_path, '..', 'input')) # Define input path relative to script location
    output_path = os.path.abspath(os.path.join(script_path, '..', 'output'))  # Define output path relative to script location
    common_meta_filepath = os.path.abspath(os.path.join(input_path, 'common_meta.xlsx')) # Define path to common metadata file
    os.chdir(script_path) # Set working directory

    data_products = prepare_data_products(common_meta_filepath) # Get and prepare data products for px file generation from Excel-sheet.
    metadata_base = prepare_metadata_base(common_meta_filepath) # Get and prepare metadata_base.
    update_folder_structure(data_products, output_path) # Create folder structure from data_products dataframe

    # Process each data product:
    for idx, row in data_products.iterrows():
        list_of_lines, px_output_path = process_data_product(row, metadata_base, input_path, output_path)
        write_px(list_of_lines, file_path=px_output_path)
    print('')

# -----------------------------------------------------------------
def prepare_data_products(common_meta_filepath):
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

    print('\nData products / tables to create px-files from:')
    print(data_products[['LEVEL_1_FOLDER', 'LEVEL_2_FOLDER', 'TABLE_NO', 'TITLE', 'STUB', 'DATA', 'UNITS']])
    if duplicates_df.shape[0] > 0:
        print('--- Duplicated table numbers (will be skipped):')
        print(duplicates_df)

    return data_products

# -----------------------------------------------------------------
def prepare_metadata_base(common_meta_filepath):
    # Prepare general metadata from Excel-sheets - common for all data products
    # Spesific values for each data product will be handled for each data product in separate .csv-files
    meta_default = file_read(common_meta_filepath, sheet_name='metadata-default')
    meta_default = meta_default[['ORDER','KEYWORD','MANDATORY','DEFAULT_VALUE','TYPE']] # Keep only relevant columns
    meta_manual = file_read(common_meta_filepath, sheet_name='metadata-manual') # Manual updates on values for some keywords (can be empty) 
    metadata_base = metadata_add(meta_default, meta_manual, 'MANUAL_VALUE') # Adds the column 'manual_value' to 'meta_default'. The value is from 'meta_manual' (match on keyword)

    return metadata_base