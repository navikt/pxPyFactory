import pandas as pd
from itertools import product
from datetime import datetime
from pxpyfactory.io_utils import get_path, file_exists, file_read
from pxpyfactory.utils import update_metadata, metadata_add, get_first_notnull, valid_value, alert_missing_mandatory, serialize_to_px_format
# import pprint

class PXDataProduct:
    def __init__(self, main_app, dp_row):
        self.main_app = main_app

        self.table_ref       = 'NAV_' + dp_row['TABLE_NO'] # Nav uses NAV_ as a prefix for table numbers
        self.table_name      = dp_row['TITLE']
        self.table_sep       = dp_row['SEP'] # Separator used in .csv-file (used for reading input file)
        self.subject_code    = dp_row['LEVEL_1_FOLDER'] # + '\\' + dp_row['LEVEL_2_FOLDER'] 
        self.subject_area    = dp_row['LEVEL_1'] # + '\\' + dp_row['LEVEL_2']

        # self.heading_list    = [] # List of all heading columns (all non-data and non-stub columns)
        self.stub_list       = [sub.strip().upper() for sub in dp_row['STUB'].split(',')]
        self.data_list       = [sub.strip().upper() for sub in dp_row['DATA'].split(',')]
        self.data_list_pure  = [sub.strip()         for sub in dp_row['DATA'].split(',')] # keep formatting of data columns since they are used as values
        self.units_list      = [sub.strip().upper() for sub in dp_row['UNITS'].split(',')]

        self.contents_var    = dp_row['CONTENTS']
        self.contvariable    = 'STAT_VAR' # data_list[0] # Column name for contents variable - can probably be anything..

        self.table_path      = get_path(self.main_app.input_path, (self.table_ref + '.csv'))
        self.table_meta_path = get_path(self.main_app.input_path, (self.table_ref + '_meta.csv'))
        self.px_output_path  = get_path(self.main_app.output_path, dp_row['LEVEL_1_FOLDER'], dp_row['LEVEL_2_FOLDER'], (self.table_ref + '.px'))

        self.list_of_lines   = [] # Final list of lines to be written to .px file


    def make_px(self):
        if file_exists(self.table_path):
            print(f"\n{self.table_ref} {self.table_name} - Start processing data product / table")
        else:
            print(f"\nWARNING: {self.table_ref} {self.table_name} - No data found. Skipping this data product / table.")
            return False

        table_data, values_dict, heading_list = self._prepare_table_data()
        metadata_prep = self.main_app.metadata_base.copy()

        # Make dict from data product info:
        manual_metadata_updates_dict = self._get_manual_metadata_updates(values_dict, heading_list)
        # Add manual data from Excel to metadata df:
        metadata_prep = update_metadata(metadata_prep, 'MANUAL_VALUE', manual_metadata_updates_dict)
        # Prepare final metadata values for this data product, and get the fill_value for missing data:
        metadata, fill_value = self._prepare_metadata_values(metadata_prep)

        data_lines = self._get_lines_of_data_from_table(table_data, values_dict, heading_list, fill_value)
        alert_missing_mandatory(metadata) # Alert if any mandatory keywords are missing values

        # Final product from each data product is the content to be written to the .px file:
        self.list_of_lines = serialize_to_px_format(metadata, data_lines)

        return True


    # Prepares a df from the data table (.csv or .parquet file)
    # + Create a dictionary with unique values for each column
    # + Create a list of all heading columns (all non-data and non-stub columns)
    def _prepare_table_data(self):
        table_data = file_read(self.table_path, sep=self.table_sep) # Fetch data table from .parquet or .csv file
        print(f"  Columns: {list(table_data.columns)}")

        values_dict = {} # Dictionary that will contain the unique values for each column in the data table
        heading_list = []
        for column in table_data.columns:
            if column not in self.data_list:
                # Ensure all values in non-data columns are strings (to avoid issues with mixed types)
                table_data[column] = table_data[column].apply(lambda x: str(x) if pd.notnull(x) else x)
                values_dict[column] = sorted(pd.unique(table_data[column]))
                if column not in self.stub_list:
                    heading_list.append(column)

        return table_data, values_dict, heading_list

    # Create a dictionary with manual metadata updates based on data product info (from Excel sheet 'dataprodukter')
    def _get_manual_metadata_updates(self, values_dict, heading_list):
        # Prepare dictionary with px-parameters from input files:
        manual_metadata_updates_dict = {
            'TABLEID':      self.table_ref     ,
            'MATRIX':       self.table_ref     ,
            'TITLE':        self.table_name    ,
            'STUB':         self.stub_list     ,
            'HEADING':      [self.contvariable] + heading_list, # Add contvariable to the list of headings,
            'CONTVARIABLE': self.contvariable  ,
            'UNITS':        '-'                , # Due to a faulty need for a plain UNITS, only the first unit is used here (unts for the rest are added later)
            'SUBJECT-CODE': self.subject_code  ,
            'SUBJECT-AREA': self.subject_area  ,
            'CONTENTS':     self.contents_var  ,
        }
        # Add px-parameter for each data column:
        for index, data_col in enumerate(self.data_list):
            manual_metadata_updates_dict['UNITS("' + data_col + '")'] = self.units_list[index] if index < len(self.units_list) else self.units_list[0] # If there is more data columns than units, use the first unit for the rest
            manual_metadata_updates_dict['LAST-UPDATED("' + data_col + '")'] = datetime.now().strftime("%Y%m%d %H:%M") # current time on px-format
            # If needed, more data-column specific px-parameters can be added. Value can be based on 'spesific_value' and implementet a few lines lower.
            # manual_metadata_updates_dict['STOCKFA("' + data_col + '")'] = 
            # manual_metadata_updates_dict['CFPRICES("' + data_col + '")'] = 
            # manual_metadata_updates_dict['DAYADJ("' + data_col + '")'] = 
            # manual_metadata_updates_dict['SEASADJ("' + data_col + '")'] = 

        for key, value in values_dict.items():
            manual_metadata_updates_dict['VALUES("' + key + '")'] = value
        manual_metadata_updates_dict['VALUES("' + self.contvariable + '")'] = self.data_list_pure

        return manual_metadata_updates_dict

    # Add metadata from spesific .csv-file, and prepare final metadata values for this data product.
    # Value used in px-file are selected from the highest priority non-null value from these three columns in metadata df: spesific_value, manual_value, default_value
    def _prepare_metadata_values(self, metadata_prep):
        # Add the column 'spesific_value' to 'metadata'. The value is from 'table_meta' (match on keyword).
        # This is spesific metadata for this data product - stored in a separate .csv-file
        if file_exists(self.table_meta_path):
            table_meta = file_read(self.table_meta_path, sep=self.table_sep)
            if {'KEYWORD', 'VALUE'}.issubset(table_meta.columns):
                print("Valid file for spesific metadata used.")
            else:
                table_meta = pd.DataFrame(columns=['KEYWORD', 'VALUE'])
        else:
            table_meta = pd.DataFrame(columns=['KEYWORD', 'VALUE'])
        metadata_prep = metadata_add(metadata_prep, table_meta, 'SPESIFIC_VALUE') # Merge the two sources of metadata
        metadata_prep['MANDATORY'] = metadata_prep['MANDATORY'].fillna('').str.lower().isin(['yes', 'nav']).astype(bool) # Mandatory column cleanup 

        # Set the value to the first non-null value in this priority:
        metadata_prep['VALUE'] = metadata_prep[['SPESIFIC_VALUE', 'MANUAL_VALUE', 'DEFAULT_VALUE']].apply(get_first_notnull, axis=1)
        metadata_prep = metadata_prep[['ORDER', 'KEYWORD', 'VALUE', 'TYPE', 'MANDATORY']] # Keep only relevant columns
        # Filter out rows with no value, unless they are mandatory:
        metadata_prep = metadata_prep[(metadata_prep['MANDATORY']) | (metadata_prep.apply(lambda row: valid_value(row['VALUE']), axis=1))].sort_values('ORDER')

        fill_value = metadata_prep.loc[metadata_prep['KEYWORD'] == 'DATASYMBOL2', 'VALUE'].iloc[0] # getting the fill value must be done after preparing the metadata values

        return metadata_prep, fill_value

    # Create the data content lines to the px-file.
    # To get the correct format all possible combinations of stub and heading values must be created, and merged with the data table.
    # Missing values are filled with the fill_value from metadata.
    def _get_lines_of_data_from_table(self, table_data, values_dict, heading_list, fill_value):
        # Create a DataFrame with all possible combinations:
        all_combinations = pd.DataFrame(list(product(*values_dict.values())), columns=self.stub_list + heading_list)
        # Merge full matrix with data, and fill missing cells:
        expanded_table_data = pd.merge(all_combinations, table_data, on=self.stub_list + heading_list, how='left').fillna(fill_value)
        # Pivot the table to get the desired format:
        data_pivot = expanded_table_data.pivot_table(index=self.stub_list, columns=heading_list, values=self.data_list, aggfunc='first')

        data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string

        return data_lines