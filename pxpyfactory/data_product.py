import pandas as pd
from hashlib import sha256
from itertools import product
import pxpyfactory.io_utils
import pxpyfactory.utils
import pxpyfactory.saved_query


class PXDataProduct:
    def __init__(self, main_app, dp_row):
        self.main_app = main_app

        self.force_build         = dp_row.pop('FORCE_BUILD')
        self.hashed_params       = sha256(dp_row.to_string().encode()).hexdigest() # Store hashed parameters to detect changes in input files

        self.table_ref           = '' + dp_row['TABLE_REF'] # Nav uses NAV_ as a prefix for table numbers
        self.table_ref_raw       = dp_row['TABLE_REF_RAW'] # Table ref from input/excel before shortening
        self.table_name          = dp_row['TITLE']
        self.table_sep           = dp_row['SEP'] # Separator used in .csv-file (used for reading input file)
        self.subject_code        = dp_row['LEVEL_1']
        self.subject_area        = dp_row['LEVEL_1'] # todo: update to show the name of the subject area

        self.stub_list           = pxpyfactory.utils.prep_list_from_string(dp_row['STUB'])
        self.heading_list        = pxpyfactory.utils.prep_list_from_string(dp_row['HEADING'])
        self.data_list           = pxpyfactory.utils.prep_list_from_string(dp_row['DATA'])
        self.data_list_pure      = pxpyfactory.utils.prep_list_from_string(dp_row['DATA'], to_upper=False) # keep formatting of data columns since they are used as values
        self.data_precision_list = pxpyfactory.utils.prep_list_from_string(dp_row['DATA'], split_part=1) # get precision part only
        self.units_list          = pxpyfactory.utils.prep_list_from_string(dp_row['UNITS'])

        self.contents_var        = dp_row['CONTENTS']
        self.contvariable        = 'STAT_VAR' # data_list[0] # Column name for contents variable - can probably be anything..

        self.table_path          = pxpyfactory.io_utils.get_path([self.main_app.input_path, self.table_ref_raw + '.csv'])
        self.table_meta_path     = pxpyfactory.io_utils.get_path([self.main_app.input_path, self.table_ref_raw + '_meta.csv'])
        self.px_output_path      = pxpyfactory.io_utils.get_path([self.main_app.output_path, dp_row['LEVEL_1'], dp_row['LEVEL_2'], self.table_ref + '.px'])
        self.sqa_output_path     = pxpyfactory.io_utils.get_path([self.main_app.output_path, 'sq', self.table_ref[0], self.table_ref + '.sqa'])
        self.sqs_output_path     = pxpyfactory.io_utils.get_path([self.main_app.output_path, 'sq', self.table_ref[0], self.table_ref + '.sqs'])

        # Common variables to be set later:
        # self.table_data          = pd.DataFrame() # DataFrame with the actual data from input file
        self.table_meta_px       = pd.DataFrame() # DataFrame with px parameters from spesific metadata file
        self.table_meta_sq       = pd.DataFrame() # DataFrame with sq parameters from spesific metadata file
        # self.values_dict         = {} # Dictionary with unique values for each column in the data table
        self.rename_map          = {} # Dictionary mapping original column names to renamed ones

        # self.list_of_lines       = [] # Final list of lines to be written to .px file

    # _____________________________________________________________________________
    def make_px(self):
        pxpyfactory.utils.print_filter(f"{self.table_ref} {self.table_name}", 1)

        if not pxpyfactory.io_utils.file_exists(self.table_path):
            pxpyfactory.utils.print_filter(f"WARNING: No data found. Skipping this data product / table.", 1)
            return False
        
        self.table_data = pxpyfactory.io_utils.file_read(self.table_path, sep=self.table_sep) # Fetch data table from .parquet or .csv file
        table_meta = pxpyfactory.io_utils.file_read(self.table_meta_path, sep=self.table_sep) # Read spesific metadata from .csv-file if it exists
        self._extract_table_metadata(table_meta) # Extract metadata from table_meta

        # Set stub, heading and data columns if not set, based on data table content
        # Create a dictionary with unique values for each column and ensure correct formatting of content
        self.values_dict = self._prepare_table_data()

        # Make dict from data product info:
        manual_metadata_updates_dict = self._get_manual_metadata_updates(self.values_dict)
        # Merge common metadata base with manual metadata (manual metadata are placed in 'MANUAL_VALUE' column):
        metadata_prep = pxpyfactory.utils.update_metadata(self.main_app.metadata_base.copy(), 'MANUAL_VALUE', manual_metadata_updates_dict)

        # Prepare final metadata values for this data product, and get the fill_value for missing data:
        # This is spesific metadata for this data product - stored in a separate .csv-file
        metadata, fill_value = self._prepare_metadata_values(metadata_prep, self.table_meta_px)

        data_lines = self._get_lines_of_data_from_table(self.values_dict, fill_value)
        pxpyfactory.utils.alert_missing_mandatory(metadata) # Alert if any mandatory keywords are missing values

        # Final product from each data product is the content to be written to the .px file:
        self.list_of_lines = pxpyfactory.utils.serialize_to_px_format(metadata, data_lines)
        return True

    # _____________________________________________________________________________
    # Generate saved query files (.sqa and .sqs) for this data product
    def make_sq(self):
        # Generate .sqa content
        sqa_content = pxpyfactory.saved_query.generate_sqa_content(
            self,
            table_id=self.table_ref,
            stub_list=self.stub_list,
            heading_list=self.heading_list,
            data_list=self.data_list,
            values_dict=self.values_dict,
            contvariable=self.contvariable,
            language="no"
        )
        sqa_ok = pxpyfactory.io_utils.file_write(self.sqa_output_path, sqa_content)
        
        # Generate .sqs content
        sqs_content = pxpyfactory.saved_query.generate_sqs_content()
        sqs_ok = pxpyfactory.io_utils.file_write(self.sqs_output_path, sqs_content)

        if sqa_ok and sqs_ok:
            return True
    # _____________________________________________________________________________
    # _____________________________________________________________________________
    # Extract metadata from table_meta to spesific PX parameters, renaming of columns, and Saved Query parameters
    # use table_meta created in make_px()
    def _extract_table_metadata(self, table_meta):
        if {'TYPE', 'KEYWORD', 'VALUE'}.issubset(table_meta.columns):
            pxpyfactory.utils.print_filter("Valid file for spesific metadata.", 2)
        else:
            pxpyfactory.utils.print_filter("Missing or unvalid file for spesific metadata.", 2)
            table_meta = table_meta.reindex(columns=table_meta.columns.tolist() + ['TYPE', 'KEYWORD', 'VALUE'], fill_value=None) # add missing columns to avoid faults
        table_meta['TYPE'] = table_meta['TYPE'].apply(str.upper)
        table_meta['KEYWORD'] = table_meta['KEYWORD'].apply(str.upper)

        self.table_meta_px      = table_meta[table_meta['TYPE'] == 'PX'][['KEYWORD', 'VALUE']] # create df with only px parameters
        self.table_meta_sq      = table_meta[table_meta['TYPE'] == 'SQ'][['KEYWORD', 'VALUE']] # create df with only sq parameters
        table_meta_rename  = table_meta[table_meta['TYPE'] == 'RENAME'][['KEYWORD', 'VALUE']] # create df with only px parameters

        # Build rename map from metadata
        for _, row in table_meta_rename.iterrows():
            keyword = row['KEYWORD']
            new_name = row['VALUE']
            self.rename_map[keyword] = new_name

    # _____________________________________________________________________________
    def _prepare_table_data(self):
        # Update stub_list, heading_list and data_list based on content of table_data if any of the lists are empty
        remaining_columns = self.table_data.columns.tolist()
        pxpyfactory.utils.print_filter(f"  All columns in table: {remaining_columns}", 2)

        pxpyfactory.utils.print_filter(f"  Initial data_list: {self.data_list}", 2)
        if pxpyfactory.utils.is_list_empty(self.data_list):
            uniqe_values_in_columns = {}
            for column in remaining_columns:
                uniqe_values_in_columns[column] = len(pd.unique(self.table_data[column]))
            # find column with most unique values and set as data column
            data_column = max(uniqe_values_in_columns, key=uniqe_values_in_columns.get)
            self.data_list = [data_column]
            remaining_columns.remove(data_column)
            for key, value in uniqe_values_in_columns.items():
                if (key in remaining_columns) and (value > 1000):
                    self.data_list.append(key)
                    remaining_columns.remove(key)
            self.data_list_pure = self.data_list
        else:
            for column in self.data_list:
                if column in remaining_columns:
                    remaining_columns.remove(column)
        pxpyfactory.utils.print_filter(f"  Updated data_list: {self.data_list}", 2)

        pxpyfactory.utils.print_filter(f"  Initial stub_list: {self.stub_list}", 2)
        if pxpyfactory.utils.is_list_empty(self.stub_list):
            for column in remaining_columns:
                if column not in self.heading_list:
                    self.stub_list = [column]
                    remaining_columns.remove(column)
                    break # only pick the first column found as stub
        else:
            for column in self.stub_list:
                if column in remaining_columns:
                    remaining_columns.remove(column)
        pxpyfactory.utils.print_filter(f"  Updated stub_list: {self.stub_list}", 2)

        pxpyfactory.utils.print_filter(f"  Initial heading_list: {self.heading_list}", 2)
        if pxpyfactory.utils.is_list_empty(self.heading_list):
            self.heading_list = []
        # Add all remaining columns must be headings:
        for column in remaining_columns:
            if column not in self.heading_list:
                self.heading_list.append(column)
        pxpyfactory.utils.print_filter(f"  Updated heading_list: {self.heading_list}", 2)

        # Apply renames to all lists
        rename_map = self.rename_map
        if rename_map:
            self.data_list = [rename_map.get(item, item) for item in self.data_list]
            self.stub_list = [rename_map.get(item, item) for item in self.stub_list]
            self.heading_list = [rename_map.get(item, item) for item in self.heading_list]
            self.units_list = [rename_map.get(item, item) for item in self.units_list]
            self.contvariable = rename_map.get(self.contvariable, self.contvariable)

        # _____________________________________________________________________________
        # Create a dictionary with unique values for each column
        # + ensure correct formatting of content
        if rename_map:
            self.table_data.rename(columns=self.rename_map, inplace=True)
        values_dict = {} # Dictionary that will contain the unique values for each column in the data table
        for column in self.table_data.columns:
            if column in self.data_list:
                # Ensure all decimal commas are replaced with dots in data columns
                self.table_data[column] = self.table_data[column].apply(lambda x: x.replace(',', '.') if isinstance(x, str) and ',' in x else x)
                self.table_data[column] = self.table_data[column].apply(lambda x: x.replace(' ', '_') if isinstance(x, str) and ' ' in x else x)
            else:
                # Ensure all values in non-data columns are strings (to avoid issues with mixed types):
                self.table_data[column] = self.table_data[column].apply(lambda x: str(x) if pd.notnull(x) else '')
                # Store unique values in column for use in VALUES-keywords:
                values_dict[column] = sorted(pd.unique(self.table_data[column]))
        return values_dict

    # _____________________________________________________________________________
    # Create a dictionary with manual metadata updates based on data product info (from Excel sheet 'dataprodukter')
    def _get_manual_metadata_updates(self, values_dict):
        # Prepare dictionary with px-parameters from input files:
        manual_metadata_updates_dict = {
            'TABLEID':      self.table_ref     ,
            'MATRIX':       self.table_ref     ,
            'TITLE':        self.table_name    ,
            'STUB':         self.stub_list     ,
            'HEADING':      [self.contvariable] + self.heading_list, # Add contvariable to the list of headings,
            'CONTVARIABLE': self.contvariable  ,
            'UNITS':        '-'                , # Due to a faulty need for a plain UNITS, only the first unit is used here (unts for the rest are added later)
            'SUBJECT-CODE': self.subject_code  ,
            'SUBJECT-AREA': self.subject_area  ,
            'CONTENTS':     self.contents_var  ,
        }
        manual_metadata_updates_dict['VALUES("' + self.contvariable + '")'] = self.data_list #_pure # Values for contents variable must be added first. Must also be in correct formatting
        for key, value in values_dict.items():
            manual_metadata_updates_dict['VALUES("' + key + '")'] = value

        # Add px-parameter for each data column:
        for index, data_col in enumerate(self.data_list):
            # If there is more data columns than units, use the first unit for the rest
            if self.units_list == []:
                units_value = ''
            elif index < len(self.units_list):
                units_value = self.units_list[index]
            else:
                units_value = self.units_list[0]

            manual_metadata_updates_dict['UNITS("' + data_col + '")'] = units_value
            manual_metadata_updates_dict['LAST-UPDATED("' + data_col + '")'] = pxpyfactory.utils.get_time_formatted() # current time on px-format

            try:
                data_precision = int(self.data_precision_list[index])
            except Exception:
                data_precision = None
            if data_precision is not None:
                manual_metadata_updates_dict['PRECISION("' + self.contvariable + '", "' + data_col + '")'] = data_precision
            
            # If needed, more data-column specific px-parameters can be added. Value can be based on 'spesific_value' and implementet a few lines lower.
            # manual_metadata_updates_dict['STOCKFA("' + data_col + '")'] = 
            # manual_metadata_updates_dict['CFPRICES("' + data_col + '")'] = 
            # manual_metadata_updates_dict['DAYADJ("' + data_col + '")'] = 
            # manual_metadata_updates_dict['SEASADJ("' + data_col + '")'] = 

        return manual_metadata_updates_dict

    # _____________________________________________________________________________
    # Add metadata from spesific .csv-file, and prepare final metadata values for this data product.
    # Value used in px-file are selected from the highest priority non-null value from these three columns in metadata df: spesific_value, manual_value, default_value
    def _prepare_metadata_values(self, metadata_prep, table_meta_px):
        # Add the column 'spesific_value' to 'metadata'. The value is from 'table_meta' (match on keyword).

        metadata_prep = pxpyfactory.utils.metadata_add(metadata_prep, table_meta_px, 'SPESIFIC_VALUE') # Merge the two sources of metadata
        metadata_prep['MANDATORY'] = metadata_prep['MANDATORY'].fillna('').str.lower().isin(['yes', 'nav']).astype(bool) # Mandatory column cleanup 

        # Set the value to the first non-null value in this priority:
        metadata_prep['VALUE'] = metadata_prep[['SPESIFIC_VALUE', 'MANUAL_VALUE', 'DEFAULT_VALUE']].apply(pxpyfactory.utils.get_first_notnull, axis=1)
        metadata_prep = metadata_prep[['ORDER', 'KEYWORD', 'VALUE', 'TYPE', 'MANDATORY']] # Keep only relevant columns
        # Filter out rows with no value, unless they are mandatory:
        metadata_prep = metadata_prep[(metadata_prep['MANDATORY']) | (metadata_prep.apply(lambda row: pxpyfactory.utils.valid_value(row['VALUE']), axis=1))].sort_values('ORDER')

        fill_value = metadata_prep.loc[metadata_prep['KEYWORD'] == 'DATASYMBOL2', 'VALUE'].iloc[0] # getting the fill value must be done after preparing the metadata values

        return metadata_prep, fill_value

    # _____________________________________________________________________________
    # Create the data content lines to the px-file.
    # To get the correct format all possible combinations of stub and heading values must be created, and merged with the data table.
    # Missing values are filled with the fill_value from metadata.
    def _get_lines_of_data_from_table(self, values_dict, fill_value):
        # 1. Generate all possible combinations in the correct order
        stub_and_headings = self.stub_list + self.heading_list
        all_combinations_of_stub_heading = pd.DataFrame(list(product(*[values_dict[axis] for axis in stub_and_headings])), columns=stub_and_headings)
        pxpyfactory.utils.print_filter(all_combinations_of_stub_heading, 3)
        # 2. Merge with actual data
        expanded_table_data = pd.merge(all_combinations_of_stub_heading, self.table_data, on=stub_and_headings, how='left').fillna(fill_value)
        pxpyfactory.utils.print_filter(expanded_table_data, 3)

        data_pivot = expanded_table_data.pivot_table(index=self.stub_list, columns=self.heading_list, values=self.data_list, aggfunc='first')
        data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string

        return data_lines