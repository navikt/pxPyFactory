import pandas as pd
import hashlib
from itertools import product
from pxpyfactory.io_utils import get_path, file_exists, file_read, get_file_info, write_log, file_write
from pxpyfactory.utils import prep_list_from_string, update_metadata, metadata_add, get_first_notnull, valid_value, is_list_empty, alert_missing_mandatory, serialize_to_px_format, get_time_formatted, same_value, print_filter
from pxpyfactory.saved_query import generate_sqa_content, generate_sqs_content
# import pprint

class PXDataProduct:
    def __init__(self, main_app, dp_row):
        self.main_app = main_app

        self.force_build         = dp_row.pop('FORCE_BUILD')
        self.hashed_params       = hashlib.sha256(dp_row.to_string().encode()).hexdigest() # Store hashed parameters to detect changes in input files

        self.table_ref           = '' + dp_row['TABLE_REF'] # Nav uses NAV_ as a prefix for table numbers
        self.table_ref_raw       = dp_row['TABLE_REF_RAW'] # Table ref from input/excel before shortening
        self.table_name          = dp_row['TITLE']
        self.table_sep           = dp_row['SEP'] # Separator used in .csv-file (used for reading input file)
        self.subject_code        = dp_row['LEVEL_1']
        self.subject_area        = dp_row['LEVEL_1'] # todo: update to show the name of the subject area

        self.stub_list           = prep_list_from_string(dp_row['STUB'])
        self.heading_list        = prep_list_from_string(dp_row['HEADING'])
        self.data_list           = prep_list_from_string(dp_row['DATA'])
        self.data_list_pure      = prep_list_from_string(dp_row['DATA'], to_upper=False) # keep formatting of data columns since they are used as values
        self.data_precision_list = prep_list_from_string(dp_row['DATA'], split_part=1) # get precision part only
        self.units_list          = prep_list_from_string(dp_row['UNITS'])

        self.contents_var        = dp_row['CONTENTS']
        self.contvariable        = 'STAT_VAR' # data_list[0] # Column name for contents variable - can probably be anything..

        self.table_path          = get_path([self.main_app.input_path, self.table_ref_raw + '.csv'])
        self.table_meta_path     = get_path([self.main_app.input_path, self.table_ref_raw + '_meta.csv'])
        self.px_output_path      = get_path([self.main_app.output_path, dp_row['LEVEL_1'], dp_row['LEVEL_2'], self.table_ref + '.px'])
        self.sqa_output_path     = get_path([self.main_app.output_path, 'sq', self.table_ref[0], self.table_ref + '.sqa'])
        self.sqs_output_path     = get_path([self.main_app.output_path, 'sq', self.table_ref[0], self.table_ref + '.sqs'])

        self.list_of_lines       = [] # Final list of lines to be written to .px file

    # _____________________________________________________________________________
    def make_px(self):
        print_filter(f"{self.table_ref} {self.table_name}", 1)

        if not file_exists(self.table_path):
            print_filter(f"WARNING: No data found. Skipping this data product / table.", 1)
            return False
        
        # if not self._input_changed() and self.force_build != True:
        #     print_filter(f"INFO: No changes in input files since last run. Skipping this data product / table.", 1)
        #     return False
        
        self.table_data = file_read(self.table_path, sep=self.table_sep) # Fetch data table from .parquet or .csv file
        self._set_columns() # Set stub, heading and data columns if not set, based on data table content
        self.values_dict = self._prepare_table_data() # Create a dictionary with unique values for each column and ensure correct formatting of content

        # Make dict from data product info:
        manual_metadata_updates_dict = self._get_manual_metadata_updates(self.values_dict)
        # Merge common metadata base with manual metadata (manual metadata are placed in 'MANUAL_VALUE' column):
        metadata_prep = update_metadata(self.main_app.metadata_base.copy(), 'MANUAL_VALUE', manual_metadata_updates_dict)
        # Prepare final metadata values for this data product, and get the fill_value for missing data:
        metadata, fill_value = self._prepare_metadata_values(metadata_prep)

        data_lines = self._get_lines_of_data_from_table(self.values_dict, fill_value)
        alert_missing_mandatory(metadata) # Alert if any mandatory keywords are missing values

        # Final product from each data product is the content to be written to the .px file:
        self.list_of_lines = serialize_to_px_format(metadata, data_lines)

        return True
    # _____________________________________________________________________________
    # Generate saved query files (.sqa and .sqs) for this data product
    def make_sq(self):
        print(f"Generate Sqved Query for {self.table_ref}")
        
        # Generate .sqa content
        sqa_content = generate_sqa_content(
            table_id=self.table_ref,
            stub_list=self.stub_list,
            heading_list=self.heading_list,
            data_list=self.data_list,
            values_dict=self.values_dict,
            language="no"
        )
        file_write(self.sqa_output_path, sqa_content)
        
        # Generate .sqs content
        sqs_content = generate_sqs_content()
        file_write(self.sqs_output_path, sqs_content)
     # _____________________________________________________________________________
    # Log production of current px file to production_log.jsonl
    def log_file_production(self):
        size, time = get_file_info(self.table_path)
        meta_size, meta_time = get_file_info(self.table_meta_path)
        current_entry_dict = {
            'table_ref': self.table_ref,
            'timestamp': get_time_formatted(),
            'hashed_params': self.hashed_params,
            'file_size': size if size is not None else '-',
            'mod_time': get_time_formatted(time) if time is not None else '-',
            'meta_file_size': meta_size if meta_size is not None else '-',
            'meta_mod_time': get_time_formatted(meta_time) if meta_time is not None else '-'
        }
        # self.main_app.production_log.loc[len(self.main_app.production_log)] = current_entry_dict
        self.main_app.production_log = pd.concat([self.main_app.production_log, pd.DataFrame([current_entry_dict])], ignore_index=True)
        if write_log(self.main_app.production_log_filepath, self.main_app.production_log):
            # File append successful+
            return True
        else:
            return False
        
    # _____________________________________________________________________________
    # Compare current input to production, with input to latest production of the same table
    def _input_changed(self):
        prod_log = self.main_app.production_log
        try:
            latest_entry = prod_log[prod_log['table_ref'] == self.table_ref].sort_values('timestamp').iloc[-1]
        except Exception as e:
            print_filter(f"No prior production logged.", 2)
            return True
        output_str = (f"Latest production was\n{latest_entry}\nFirst input update:")
        size, time = get_file_info(self.table_path)
        meta_size, meta_time = get_file_info(self.table_meta_path)
        if not same_value(latest_entry['hashed_params'], self.hashed_params):
            print_filter(f"{output_str} hashed_params: '{latest_entry['hashed_params']}' -> '{self.hashed_params}'", 2)
            return True
        if not same_value(latest_entry['file_size'], size):
            print_filter(f"{output_str} file_size: '{latest_entry['file_size']}' -> '{size}'", 2)
            return True
        if not same_value(latest_entry['mod_time'], time):
            print_filter(f"{output_str} mod_time: '{latest_entry['mod_time']}' -> '{time}'", 2)
            return True
        if not same_value(latest_entry['meta_file_size'], meta_size):
            print_filter(f"{output_str} meta_file_size: '{latest_entry['meta_file_size']}' -> '{meta_size}'", 2)
            return True
        if not same_value(latest_entry['meta_mod_time'], meta_time):
            print_filter(f"{output_str} meta_mod_time : '{latest_entry['meta_mod_time']}' -> '{meta_time}'", 2)
            return True

        return False
    # _____________________________________________________________________________
    # Update stub_list, heading_list and data_list based on content of table_data if any of the lists are empty
    def _set_columns(self):
        remaining_columns = self.table_data.columns.tolist()
        print_filter(f"  All columns in table: {remaining_columns}", 2)

        print_filter(f"  Initial data_list: {self.data_list}", 2)
        if is_list_empty(self.data_list):
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
        print_filter(f"  Updated data_list: {self.data_list}", 2)

        print_filter(f"  Initial stub_list: {self.stub_list}", 2)
        if is_list_empty(self.stub_list):
            for column in remaining_columns:
                if column not in self.heading_list:
                    self.stub_list = [column]
                    remaining_columns.remove(column)
                    break # only pick the first column found as stub
        else:
            for column in self.stub_list:
                if column in remaining_columns:
                    remaining_columns.remove(column)
        print_filter(f"  Updated stub_list: {self.stub_list}", 2)

        print_filter(f"  Initial heading_list: {self.heading_list}", 2)
        if is_list_empty(self.heading_list):
            self.heading_list = []
        # Add all remaining columns must be headings:
        for column in remaining_columns:
            if column not in self.heading_list:
                self.heading_list.append(column)
        print_filter(f"  Updated heading_list: {self.heading_list}", 2)


    # _____________________________________________________________________________
    # Create a dictionary with unique values for each column
    # + ensure correct formatting of content
    def _prepare_table_data(self):
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
            manual_metadata_updates_dict['LAST-UPDATED("' + data_col + '")'] = get_time_formatted() # current time on px-format

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
    def _prepare_metadata_values(self, metadata_prep):
        # Add the column 'spesific_value' to 'metadata'. The value is from 'table_meta' (match on keyword).
        # This is spesific metadata for this data product - stored in a separate .csv-file
        if file_exists(self.table_meta_path):
            table_meta = file_read(self.table_meta_path, sep=self.table_sep)
            if {'KEYWORD', 'VALUE'}.issubset(table_meta.columns):
                print_filter("Valid file for spesific metadata used.", 2)
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

    # _____________________________________________________________________________
    # Create the data content lines to the px-file.
    # To get the correct format all possible combinations of stub and heading values must be created, and merged with the data table.
    # Missing values are filled with the fill_value from metadata.
    def __get_lines_of_data_from_table(self, values_dict, fill_value):
        # Create a DataFrame with all possible combinations:
        # Merge full matrix with data, and fill missing cells:
        expanded_table_data = pd.merge(all_combinations, self.table_data, on=self.stub_list + self.heading_list, how='left').fillna(fill_value)
        # Pivot the table to get the desired format:
        data_pivot = expanded_table_data.pivot_table(index=self.stub_list, columns=self.heading_list, values=self.data_list, aggfunc='first')

        data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string

        return data_lines

# _____________________________________________________________________________
    def new_get_lines_of_data_from_table(self, values_dict, fill_value):
        # Match list items to actual column names (case-insensitive)
        actual_columns = self.table_data.columns.tolist()
        columns_map = {col.upper(): col for col in actual_columns}
        
        stub_list_actual = [columns_map.get(col.upper(), col) for col in self.stub_list]
        heading_list_actual = [columns_map.get(col.upper(), col) for col in self.heading_list]
        data_list_actual = [columns_map.get(col.upper(), col) for col in self.data_list]
        
        # 1. Generate all possible combinations in the correct order
        stub_and_headings = stub_list_actual + heading_list_actual
        all_combinations_of_stub_heading = pd.DataFrame(list(product(*[values_dict[axis] for axis in stub_and_headings])), columns=stub_and_headings)
        print_filter(all_combinations_of_stub_heading, 3)
        # 2. Merge with actual data
        expanded_table_data = pd.merge(all_combinations_of_stub_heading, self.table_data, on=stub_and_headings, how='left').fillna(fill_value)
        print_filter(expanded_table_data, 3)

        data_pivot = expanded_table_data.pivot_table(index=stub_list_actual, columns=heading_list_actual, values=data_list_actual, aggfunc='first')
        data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string

    def _get_lines_of_data_from_table(self, values_dict, fill_value):

        print('==='*50)
        print(f"values_dict: {values_dict}")
        print(f"stub_list: {self.stub_list}")
        print(f"heading_list: {self.heading_list}")
        print(f"data_list: {self.data_list}")
        print(f"table_data: {self.table_data}")
        print('==='*50)

        # 1. Generate all possible combinations in the correct order
        stub_and_headings = self.stub_list + self.heading_list
        all_combinations_of_stub_heading = pd.DataFrame(list(product(*[values_dict[axis] for axis in stub_and_headings])), columns=stub_and_headings)
        print_filter(all_combinations_of_stub_heading, 3)
        # 2. Merge with actual data
        expanded_table_data = pd.merge(all_combinations_of_stub_heading, self.table_data, on=stub_and_headings, how='left').fillna(fill_value)
        print_filter(expanded_table_data, 3)

        data_pivot = expanded_table_data.pivot_table(index=self.stub_list, columns=self.heading_list, values=self.data_list, aggfunc='first')
        data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string


        # # 3. For each row, extract the data columns in the order of self.data_list
        # data_lines = []
        # for _, row in expanded_table_data.iterrows():
        #     # If you have multiple data columns, you may want to join them all
        #     values = [row[data_col] for data_col in self.data_list]
        #     data_lines.append(' '.join(map(str, values)))

        return data_lines