import pandas as pd
import os
from datetime import datetime
from pxpyfactory.utils import *
from pxpyfactory.file_utils import *

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
    folder_dict   = data_products.groupby('LEVEL_1')['LEVEL_2'].unique().apply(list).to_dict()
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
    manual_meta_desc = file_read(common_meta_filepath, sheet_name='metadata-desc') # All keywords and agreed setup
    manual_meta      = file_read(common_meta_filepath, sheet_name='metadata') # Manual updates on values for some keywords (can be empty) 
    metadata_base    = metadata_add(manual_meta_desc, manual_meta, 'MANUAL_VALUE') # Merge the two sources of metadata.
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
        data_column     = row['DATA'].upper()
        # data_list     = [sub.strip().upper() for sub in row['DATA'].split(',')]
        stub_list       = [sub.strip().upper() for sub in row['STUB'].split(',')]
        units_var       = row['UNITS']
        contents_var    = row['CONTENTS']

        print(f"--- Start processing data product / table: {table_ref} {table_name}")

        table_data = file_read(table_path, sep=',') # Fetch data table from .parquet or .csv file
        # table_data.columns = [col.upper() for col in table_data.columns] # Standardize column names to uppercase
        for column in table_data.columns:
            # column = column.upper()
            if column != data_column:
            # if column not in data_list:
                table_data[column] = table_data[column].apply(lambda x: str(x) if pd.notnull(x) else x)

        column_list = list(table_data.columns) # Fetch column list from data table
        column_list.remove(data_column)
        heading_list = [column for column in column_list if column not in stub_list] # ? use instead: [sub.strip().upper() for sub in row['Delvariabler'].split(',')]
        
        values_dict = {}
        for column in column_list:
            # unique_values = list(set(table_data[column]))
            # values_dict[column] = [str(x) for x in unique_values]
            values_dict[column] = list(set(table_data[column]))

        # Calculate dimentions of the data matrix
        y_dim = 1
        x_dim = 1
        for key in values_dict.keys():
        # for key in dict.keys(values_dict):
            if key in stub_list:
                x_dim *= len(values_dict[key])
            if key in heading_list:
                y_dim *= len(values_dict[key])

        # Get all unique values for each stub and heading column
        stub_values = [values_dict[stub] for stub in stub_list]
        heading_values = [values_dict[heading] for heading in heading_list]

        # Create a MultiIndex of all possible combinations
        all_combinations = pd.MultiIndex.from_product(stub_values + heading_values, names=stub_list + heading_list)

        # Set index to stub + heading columns, reindex to include all combinations, and sort index
        table_indexed = table_data.set_index(stub_list + heading_list).reindex(all_combinations).sort_index()
        
        # Fill NaN with 0 or any other placeholder if needed, and Reshape to 2D array
        data_array = table_indexed[data_column].fillna(0).to_numpy()
        data_out = pd.DataFrame(data_array.reshape(x_dim, y_dim), dtype=float)

        try:
            table_meta = file_read(table_meta_path)
        except Exception:
            table_meta = pd.DataFrame(columns=['KEYWORD', 'VALUE'])
        metadata = metadata_add(metadata_base.copy(), table_meta, 'SPESIFIC_VALUE')
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
        # Print all keyword and value pairs that will be used in the PX file
        meta_out = metadata.copy()[(metadata['MANDATORY']) | (metadata.apply(lambda row: valid_value(row['VALUE']), axis=1))][['ORDER', 'KEYWORD', 'VALUE', 'TYPE', 'MANDATORY']].sort_values('ORDER')
        alert_missing_mandatory(meta_out) # Alert if any mandatory keywords are missing values
        list_of_lines = prepare_px_lines(meta_out, data_out) # Just print it out for now
        write_px(list_of_lines, file_path=px_output_path)

        print(f"--- End processing data product / table.")
