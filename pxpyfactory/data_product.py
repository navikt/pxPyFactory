import re
import copy
import pandas as pd
from hashlib import sha256
from itertools import product
import pxpyfactory.file_io
import pxpyfactory.saved_query
import pxpyfactory.helpers
import pxpyfactory.validation
import pxpyfactory.keyword_contact


class PXDataProduct:
    def __init__(self, main_app, dp_row):
        self.main_app = main_app

        self.force_build         = dp_row.pop('FORCE_BUILD')
        self.hashed_params       = sha256(dp_row.to_string().encode()).hexdigest() # Store hashed parameters to detect changes in input files

        self.tableid             = '' + dp_row['TABLEID'] # Nav uses NAV_ as a prefix for table numbers
        self.tableid_raw         = dp_row['TABLEID_RAW'] # Table ref from input/excel before shortening
        self.table_name          = dp_row['TITLE']
        self.subject_code        = dp_row['SUBJECT-CODE'] if pxpyfactory.validation.valid_value(dp_row['SUBJECT-CODE']) else dp_row['SUBJECT-AREA']  
        self.subject_area        = dp_row['SUBJECT-AREA'] # todo: update to show the name of the subject area

        self.stub_list           = pxpyfactory.helpers.prep_list_from_string(dp_row['STUB'])
        self.heading_list        = pxpyfactory.helpers.prep_list_from_string(dp_row['HEADING'])
        self.data_list           = pxpyfactory.helpers.prep_list_from_string(dp_row['DATA'])
        # self.data_list_pure      = pxpyfactory.helpers.prep_list_from_string(dp_row['DATA'], to_upper=False) # keep formatting of data columns since they are used as values
        self.data_precision_list = pxpyfactory.helpers.prep_list_from_string(dp_row['DATA'], split_part=1) # get precision part only
        self.units_list          = pxpyfactory.helpers.prep_list_from_string(dp_row['UNITS'])
        self.timeval_list        = pxpyfactory.helpers.prep_list_from_string(dp_row['TIMEVAL'])

        self.contents_var        = dp_row['CONTENTS']
        self.contvariable        = 'STAT_VAR' # data_list[0] # Column name for contents variable - can probably be anything..

        self.table_path          = "/".join([self.main_app.input_path, self.tableid_raw + '.csv'])
        self.table_meta_path     = "/".join([self.main_app.input_path, self.tableid_raw + '_meta.csv'])
        self.px_output_path      = "/".join([self.main_app.output_path, dp_row['SUBJECT-AREA'], dp_row['SUBJECT'], self.tableid + '.px'])
        self.sqa_output_path     = "/".join([self.main_app.sq_output_path, self.tableid[0], self.tableid + '.sqa'])
        self.sqs_output_path     = "/".join([self.main_app.sq_output_path, self.tableid[0], self.tableid + '.sqs'])

        # self.keywords            = {}

        # Common variables to be set later:
        # self.table_data          = pd.DataFrame() # DataFrame with the actual data from input file
        # self.table_meta_px       = pd.DataFrame() # DataFrame with px parameters from spesific metadata file
        self.table_meta_sq       = pd.DataFrame() # DataFrame with sq parameters from spesific metadata file (this is used in the SavedQueryGenerator)
        self.values_dict         = {} # Dictionary with unique values for each column in the data table (this is also used in the SavedQueryGenerator)
        self.rename_map          = {} # Dictionary mapping original column names to renamed ones
        # self.translation_map     = main_app.translation_base.copy() # DataFrame mapping original text to translations for different languages (built from metadata with language-specific renames)

        self.list_of_lines       = [] # Final list of lines to be written to .px file

        # self.fill_value          = None # Value used to fill empty cells in data table; this is needed to be able to get unique values for each column without losing information about empty cells, and must be set before preparing the data lines from the data table

    # _____________________________________________________________________________
    def create_px_content(self):
        pxpyfactory.helpers.print_filter(f"{self.tableid} {self.table_name}", 1)

        if not pxpyfactory.file_io.file_exists(self.table_path):
            pxpyfactory.helpers.print_filter(f"WARNING: No data found. Skipping this data product / table.", 1)
            return False
        
        table_data = pxpyfactory.file_io.file_read(self.table_path) # Fetch data table from .parquet or .csv file
        table_data, self.stub_list, self.heading_list, self.data_list = self._prepare_columns(table_data, self.stub_list, self.heading_list, self.data_list) 

        # Get unique values for each column in the data table to be able to create all combinations of stub and heading values later.
        self.values_dict = self._create_values_dict(table_data, self.data_list) 

        # Update keyword instances with spesific metadata for this data product.
        keywords = self._set_keywords_base_for_data_product() # OBS # This function uses a lot of self variables.

        table_meta = pxpyfactory.file_io.file_read(self.table_meta_path) # Read spesific metadata from .csv-file if it exists
        table_meta_px, self.table_meta_sq, self.table_meta_rename = self._extract_table_metadata(table_meta) # Extract spesific metadata from table_meta

        # Update keyword values from spesific metadata for this data product (table_meta_px)
        keywords = self._update_keywords_with_spesific_metadata(keywords, table_meta_px)

        # Get px-lines for all keywords. Requested languages are included.
        meta_lines = self._get_all_px_lines_from_keywords(keywords, keywords['LANGUAGES'].get_value())
 
        # data_lines are the lines with the actual data content to be written to the .px file.
        fill_value = keywords['DATASYMBOL2'].get_value()
        data_lines = self._get_lines_of_data_from_table(table_data, self.values_dict, self.stub_list, self.heading_list, self.data_list, fill_value)

        # Combine meta lines and data lines into final list of lines to be written to .px file
        self.list_of_lines = meta_lines + ['DATA='] + data_lines + [';']
    
        return True

    # _____________________________________________________________________________
    # Generate saved query files (.sqa and .sqs) for this data product
    def make_sq(self):
        sq_generator = pxpyfactory.saved_query.SavedQueryGenerator(self)
        
        sqa_content = sq_generator.generate_sqa()
        sqa_ok = pxpyfactory.file_io.file_write(self.sqa_output_path, sqa_content)
        
        sqs_content = sq_generator.generate_sqs()
        sqs_ok = pxpyfactory.file_io.file_write(self.sqs_output_path, sqs_content)

        if sqa_ok and sqs_ok:
            return True
    # _____________________________________________________________________________
    # _____________________________________________________________________________
    def _prepare_columns(self, table_data, stub_list, heading_list, data_list):
        # Update stub_list, heading_list and data_list based on content of table_data if any of the lists are empty
        remaining_columns = table_data.columns.tolist()
        pxpyfactory.helpers.print_filter(f"  All columns in table: {remaining_columns}", 2)

        pxpyfactory.helpers.print_filter(f"  Initial data_list: {data_list}", 2)
        if pxpyfactory.validation.is_list_empty(data_list):
            uniqe_values_in_columns = {}
            for column in remaining_columns:
                uniqe_values_in_columns[column] = len(pd.unique(table_data[column]))
            # find column with most unique values and set as data column
            data_column = max(uniqe_values_in_columns, key=uniqe_values_in_columns.get)
            data_list = [data_column]
            remaining_columns.remove(data_column)
            for key, value in uniqe_values_in_columns.items():
                if (key in remaining_columns) and (value > 1000):
                    data_list.append(key)
                    remaining_columns.remove(key)
            # self.data_list_pure = data_list
        else:
            for column in data_list:
                if column in remaining_columns:
                    remaining_columns.remove(column)
        pxpyfactory.helpers.print_filter(f"  Updated data_list: {data_list}", 2)

        pxpyfactory.helpers.print_filter(f"  Initial stub_list: {stub_list}", 2)
        if pxpyfactory.validation.is_list_empty(stub_list):
            for column in remaining_columns:
                if column not in heading_list:
                    stub_list = [column]
                    remaining_columns.remove(column)
                    break # only pick the first column found as stub
        else:
            for column in stub_list:
                if column in remaining_columns:
                    remaining_columns.remove(column)
        pxpyfactory.helpers.print_filter(f"  Updated stub_list: {stub_list}", 2)

        pxpyfactory.helpers.print_filter(f"  Initial heading_list: {heading_list}", 2)
        if pxpyfactory.validation.is_list_empty(heading_list):
            heading_list = []
        # Add all remaining columns must be headings:
        for column in remaining_columns:
            if column not in heading_list:
                heading_list.append(column)
        pxpyfactory.helpers.print_filter(f"  Updated heading_list: {heading_list}", 2)

        # self.rename_stuff(self.rename_map) # rename_map
        # table_data = self.rename_table_data(self.rename_map, table_data)

        # Ensure correct formatting of content
        for column in table_data.columns:
            if column in data_list:
                # Ensure all decimal commas are replaced with dots in data columns
                table_data[column] = table_data[column].apply(lambda x: x.replace(',', '.') if isinstance(x, str) and ',' in x else x)
                table_data[column] = table_data[column].apply(lambda x: x.replace(' ', '_') if isinstance(x, str) and ' ' in x else x)
            else:
                # Ensure all values in non-data columns are strings (to avoid issues with mixed types):
                table_data[column] = table_data[column].apply(lambda x: str(x) if pd.notnull(x) else '')

        return table_data, stub_list, heading_list, data_list

    # _____________________________________________________________________________
    def _create_values_dict(self, table_data, data_list):
        # Create a dictionary with unique values for each column
        values_dict = {}
        for column in table_data.columns:
            if column not in data_list:
            # Store unique values in column for use in VALUES-keywords:
                values_dict[column] = sorted(pd.unique(table_data[column]))
        return values_dict
    

    # _____________________________________________________________________________
    def _update_keywords_with_spesific_metadata(self, keywords, table_meta_px):
        # Update or add keyword values from _meta file (spesific for this data product - table_meta_px).
        # For each row in table_meta_px, the value and language are added to the corresponding keyword instance in keywords.
        # If the keyword from table_meta_px is not found in keywords, a warning is printed and the keyword is ignored.
        for _, row in table_meta_px.iterrows():
            keyword = row['KEYWORD']
            language = row['LANGUAGE']
            value = row['VALUE']
            if keyword == 'CONTACT':
                value = pxpyfactory.keyword_contact.shape_to_px(value) # Ensure correct formatting of contact information.
            if keyword in keywords:
                if keywords[keyword].language_dependent and pxpyfactory.validation.valid_value(language):
                    keywords[keyword].set_value(value, language=language) # Add value and language to the keyword instance
                else:
                    keywords[keyword].set_value(value) # Add value to the keyword instance without language
            else:
                pxpyfactory.helpers.print_filter(f"WARNING: Keyword '{keyword}' from spesific metadata not found in metadata base. This keyword will be ignored.", 1)

        return keywords

    # _____________________________________________________________________________
    # Extract metadata from table_meta to spesific PX parameters, renaming of columns, and Saved Query parameters
    # use table_meta created in make_px()
    def _extract_table_metadata(self, table_meta):
        if {'TYPE', 'KEYWORD', 'VALUE'}.issubset(table_meta.columns):
            pxpyfactory.helpers.print_filter("Valid file for spesific metadata.", 2)
        else:
            pxpyfactory.helpers.print_filter("Missing or unvalid file for spesific metadata.", 2)
            table_meta = table_meta.reindex(columns=table_meta.columns.tolist() + ['TYPE', 'KEYWORD', 'VALUE'], fill_value=None) # add missing columns to avoid faults

        # Add LANGUAGE column if it doesn't exist
        if 'LANGUAGE' not in table_meta.columns:
            table_meta['LANGUAGE'] = None
        
        # Convert to string and handle NaN values before applying upper()
        table_meta['TYPE']      = table_meta['TYPE'].astype(str).str.upper()     # uppercase
        table_meta['KEYWORD']   = table_meta['KEYWORD'].astype(str).str.upper()  # uppercase
        table_meta['LANGUAGE']  = table_meta['LANGUAGE'].astype(str).str.lower() # lowercase
        
        table_meta_px      = table_meta[table_meta['TYPE'] == 'PX'][['KEYWORD', 'LANGUAGE', 'VALUE']]      # create df with only px parameters
        table_meta_sq      = table_meta[table_meta['TYPE'] == 'SQ'][['KEYWORD', 'LANGUAGE', 'VALUE']]      # create df with only sq parameters
        table_meta_rename  = table_meta[table_meta['TYPE'] == 'RENAME'][['KEYWORD', 'LANGUAGE', 'VALUE']]  # create df with only rename parameters
        return table_meta_px, table_meta_sq, table_meta_rename
        


    # _____________________________________________________________________________
    def _set_keywords_base_for_data_product(self):
        keywords = copy.deepcopy(self.main_app.keywords_base) # Detached copy of keywords_base and keywords in it; changes here must not affect main_app.keywords_base
        language_initial = 'raw' # Language for initial values taken directly from input table or metadata without translation

        initial_keyword_values = {
            'TABLEID':       self.tableid,
            'MATRIX':        self.tableid,
            'TITLE':         self.table_name,
            'STUB':          self.stub_list,
            'HEADING':       [self.contvariable] + self.heading_list, # Add contvariable to the list of headings,
            'CONTVARIABLE':  self.contvariable,
            'SUBJECT-CODE':  self.subject_code,
            'SUBJECT-AREA':  self.subject_area,
            'CONTENTS':      self.contents_var,
        }

        for keyword_name, keyword_value in initial_keyword_values.items():
            keywords[keyword_name].set_value(keyword_value, language=language_initial)

        # Values for contents variable must be added first. Must also be in correct formatting
        keywords['VALUES'].set_value(self.data_list, scope=self.contvariable, language=language_initial) 

        for key, value in self.values_dict.items():
            keywords['VALUES'].set_value(value, scope=key, language=language_initial)
            if key in self.timeval_list:
                tlist_id = None
                check_value = value[0]
                # letters = re.sub(r'[^a-zA-Z]', '', check_value)
                if check_value.isdigit() and len(check_value) == 4:
                    tlist_id = 'A1' # Yearly data
                elif len(check_value) >= 6:
                    time_code = check_value[4]
                    if time_code == 'K':
                        tlist_id = 'Q1' # Quarterly data
                    elif time_code == 'M':
                        tlist_id = 'M1' # Monthly data
                    elif time_code == 'U':
                        tlist_id = 'W1' # Weekly data
                    # else:
                    #     tlist_id = 'x'  # Interval data
                if tlist_id is not None:
                    timeval_value = ['TLIST(' + tlist_id + ')'] + value
                    keywords['TIMEVAL'].set_value(timeval_value, scope=key, language=language_initial)

        ## Prepare px-parameters
        # If it is not stated in metadata, get last updated time from data file
        table_file_updated = pxpyfactory.file_io.get_last_updated(self.table_path)
        keywords['LAST-UPDATED'].set_value(table_file_updated, set_as_default_value=True)
        keywords['LAST-UPDATED'].strictly_enforce_scope = False
        # keywords['LAST-UPDATED'].allow_empty_return_value = False

        shaped_contact_value = pxpyfactory.keyword_contact.shape_to_px(keywords['CONTACT'].default_value) # Ensure correct formatting of contact information.
        keywords['CONTACT'].set_value(shaped_contact_value, set_as_default_value=True)
        keywords['CONTACT'].strictly_enforce_scope = False
        keywords['CONTACT'].use_default_value_as_base = True # This must be set before setting separate values.
        keywords['CONTACT'].set_value_use_append = True # This must be set before setting separate values.

        # Due to a faulty need for only plain UNITS in addition to the others, it is added here.
        keywords['UNITS'].set_value('-')
        keywords['UNITS'].scope_can_not_be_both_none_and_specific = False

        # Add px-parameter for each data column:
        for index, data_col in enumerate(self.data_list):
            # Some keyword values are set to None for each data column. This will make them find a value set for other scopes and languages or use the default value.
            keywords['LAST-UPDATED'].set_value(None, scope=data_col, language=language_initial)
            keywords['CONTACT'].set_value(None, scope=data_col, language=language_initial)

            # If there is more data columns than units, use the first unit for the rest
            if self.units_list == []:
                units_value = ''
            elif index < len(self.units_list):
                units_value = self.units_list[index]
            else:
                units_value = self.units_list[0]

            keywords['UNITS'].set_value(units_value, scope=data_col, language=language_initial)

            try:
                data_precision = int(self.data_precision_list[index])
            except Exception:
                data_precision = None
            if data_precision is not None:
                keywords['PRECISION'].set_value(data_precision, scope=(self.contvariable, data_col), language=language_initial)
            
            # If needed, more data-column specific px-parameters can be added. Value can be based on 'spesific_value' and implementet a few lines lower.
            # keywords['STOCKFA'].set_value(value, scope=data_col, language=language_initial)
            # keywords['CFPRICES'].set_value(value, scope=data_col, language=language_initial)
            # keywords['DAYADJ'].set_value(value, scope=data_col, language=language_initial)
            # keywords['SEASADJ'].set_value(value, scope=data_col, language=language_initial)

        return keywords

    # _____________________________________________________________________________
    def _get_all_px_lines_from_keywords(self, keywords, languages):
        # Get lines for all keywords to be written to the .px file. This includes handling of language-specific values and warnings for missing mandatory values.
        list_of_lines = []
        
        sorted_keywords = sorted(keywords.items(), key=lambda item: item[1].order)

        for keyword_name, keyword in sorted_keywords:
            pxpyfactory.helpers.print_filter(f"Keyword: {keyword_name} - order: {keyword.order} - value: {keyword.value} - default_value: {keyword.default_value}", 4)
            if keyword.order < 1000: # Only include keywords with order < 1000 in the .px file; the rest are used for internal handling and not part of the final .px content
                keyword_lines = keyword.get_px_lines(languages=languages, warn_on_missing_mandatory=True)
                list_of_lines.extend(keyword_lines)

        return list_of_lines

    # _____________________________________________________________________________
    # Create the data content lines to the px-file.
    # To get the correct format all possible combinations of stub and heading values must be created, and merged with the data table.
    # Missing values are filled with the provided fill_value.
    def _get_lines_of_data_from_table(self, table_data, values_dict, stub_list, heading_list, data_list, fill_value):
        # 1. Generate all possible combinations in the correct order
        stub_and_headings = stub_list + heading_list
        all_combinations_of_stub_heading = pd.DataFrame(list(product(*[values_dict[axis] for axis in stub_and_headings])), columns=stub_and_headings)
        pxpyfactory.helpers.print_filter(all_combinations_of_stub_heading, 3)
        # 2. Merge with actual data
        expanded_table_data = pd.merge(all_combinations_of_stub_heading, table_data, on=stub_and_headings, how='left').fillna(fill_value)
        pxpyfactory.helpers.print_filter(expanded_table_data, 3)

        data_pivot = expanded_table_data.pivot_table(index=stub_list, columns=heading_list, values=data_list, aggfunc='first')
        data_lines = [' '.join(map(str, row)) for row in data_pivot.values] # Convert each row to a space-separated string

        return data_lines
    

