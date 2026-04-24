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

        self.stub_list           = [] # pxpyfactory.helpers.prep_list_from_string(dp_row['STUB'])
        self.heading_list        = [] # pxpyfactory.helpers.prep_list_from_string(dp_row['HEADING'])
        self.data_list           = [] # pxpyfactory.helpers.prep_list_from_string(dp_row['DATA'])
        # self.data_list_pure      = pxpyfactory.helpers.prep_list_from_string(dp_row['DATA'], to_upper=False) # keep formatting of data columns since they are used as values
        self.data_precision_list = [] # pxpyfactory.helpers.prep_list_from_string(dp_row['DATA'], split_part=1) # get precision part only
        self.data_units_list     = [] # pxpyfactory.helpers.prep_list_from_string(dp_row['UNITS'])
        self.timeval_list        = [] # pxpyfactory.helpers.prep_list_from_string(dp_row['TIMEVAL'])

        self.contents_var        = dp_row['CONTENTS']
        self.contvariable        = 'STAT_VAR' # data_list[0] # Column name for contents variable - can probably be anything..

        self.table_path          = "/".join([self.main_app.input_path, self.tableid_raw + '.csv'])
        self.table_meta_path     = "/".join([self.main_app.input_path, self.tableid_raw + '_meta.csv'])
        self.px_output_path      = "/".join([self.main_app.output_path, dp_row['SUBJECT-AREA'], dp_row['SUBJECT'], self.tableid + '.px'])
        self.sqa_output_path     = "/".join([self.main_app.sq_output_path, self.tableid[0], self.tableid + '.sqa'])
        self.sqs_output_path     = "/".join([self.main_app.sq_output_path, self.tableid[0], self.tableid + '.sqs'])

        self.keywords            = {}

        # Common variables to be set later:
        # self.table_data          = pd.DataFrame() # DataFrame with the actual data from input file
        # self.table_meta_px       = pd.DataFrame() # DataFrame with px parameters from spesific metadata file
        self.table_meta_sq       = pd.DataFrame() # DataFrame with sq parameters from spesific metadata file (this is used in the SavedQueryGenerator)
        self.values_dict         = {} # Dictionary with unique values for each column in the data table (this is also used in the SavedQueryGenerator)

        self.list_of_lines       = [] # Final list of lines to be written to .px file

        # self.fill_value          = None # Value used to fill empty cells in data table; this is needed to be able to get unique values for each column without losing information about empty cells, and must be set before preparing the data lines from the data table

    # _____________________________________________________________________________
    def create_px_content(self):
        pxpyfactory.helpers.print_filter(f"{self.tableid} {self.table_name}", 1)

        if not pxpyfactory.file_io.file_exists(self.table_path):
            pxpyfactory.helpers.print_filter(f"WARNING: No data found. Skipping this data product / table.", 1)
            return False
        
        table_data = pxpyfactory.file_io.file_read(self.table_path) # Fetch data table from .parquet or .csv file
        table_meta = pxpyfactory.file_io.file_read(self.table_meta_path) # Read spesific metadata from .csv-file if it exists
        table_meta_cs, table_meta_px, table_meta_cr, self.table_meta_sq = self._extract_table_metadata(table_meta) # Extract spesific metadata from table_meta

        table_data, self.stub_list, self.heading_list, self.data_list, self.data_precision_list, self.data_units_list, self.timeval_list = self._prepare_columns(table_data, table_meta_cs) 

        # Get unique values for each column in the data table to be able to create all combinations of stub and heading values later.
        self.values_dict = self._create_values_dict(table_data, self.data_list) 

        # Update keyword instances with spesific metadata for this data product.
        keywords = self._set_keywords_base_for_data_product() # OBS # This function uses a lot of self variables.

        # Update keyword values from spesific metadata for this data product
        keywords = self._update_standalone_keywords(keywords, table_meta_px)

        # Update interconnected keyword values (for example adding data column names and values in different languages)
        keywords = self._update_interconnected_keywords(keywords, table_meta_cr)

        # Get px-lines for all keywords. Requested languages are included. Main language is included if that language should be without language tag in the .px file.
        meta_lines = self._get_all_px_lines_from_keywords(keywords, keywords['LANGUAGES'].get_value(), keywords['LANGUAGE'].get_value())
 
        # data_lines are the lines with the actual data content to be written to the .px file.
        fill_value = keywords['DATASYMBOL2'].get_value()
        data_lines = self._get_lines_of_data_from_table(table_data, self.values_dict, self.stub_list, self.heading_list, self.data_list, fill_value)

        # Combine meta lines and data lines into final list of lines to be written to .px file
        self.list_of_lines = meta_lines + ['DATA='] + data_lines + [';']

        self.keywords = keywords # Store keywords in self to be able to use them later when creating Saved Query files (.sqa and .sqs)
    
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
    # Update stub_list, heading_list and data_list based on content of table_data if any of the lists are empty
    # If data_list is empty, first select column(s) for data_list based on number of unique values in them.
    # Then select stub_list and heading_list based on remaining columns.
    # TIMEVAL can also be found and set (probably based on column name=TIMEVAL or TIME) if it is empty, but this funktionality is not implemented yet.
    def _prepare_columns(self, table_data, table_meta_cs):
        remaining_columns = table_data.columns.tolist().copy()
        pxpyfactory.helpers.print_filter(f"  All columns in table: {remaining_columns}", 2)

        
        def _get_columns_from_meta(cs_df, column_type, split_part=None, keep_case=False):
            columns = []
            if not isinstance(cs_df, pd.DataFrame) or not {'KEYWORD', 'VALUE'}.issubset(cs_df.columns):
                return columns
            if (cs_df['KEYWORD'] == column_type).any():
                column_str = cs_df.loc[cs_df['KEYWORD'] == column_type, 'VALUE'].iloc[0]
                for col in column_str.split(','):
                    if split_part is not None:
                        col = col.split('#')
                        if len(col) > split_part:
                            col = col[split_part]
                        else:
                            col = None
                    if col is not None:
                        col = col.strip()
                        if col.upper() in remaining_columns or split_part not in [None, 0]:
                            if not keep_case:
                                col = col.upper()
                            columns.append(col)
            return columns
        
        # Data precision can be set in metadata as a part of the DATA keyword value, separated by #.
        # For example: "VALUE=SALES#2" would set data column to SALES and data precision to 2.
        # This is implemented to be able to set data precision for each data column separately if needed.
        data_precision_list = _get_columns_from_meta(table_meta_cs, 'DATA', 1)
        data_units_list = _get_columns_from_meta(table_meta_cs, 'DATA', 2, keep_case=True) # Get units part of the DATA keyword value if it exists. Units can be set for each data column separately if needed.
        timeval_list = _get_columns_from_meta(table_meta_cs, 'TIMEVAL')

        data_list = _get_columns_from_meta(table_meta_cs, 'DATA', 0) # Update data_list based on content in spesific metadata if it exists
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
        
        # If there are more data columns than precision values, the remaining data columns will have None as data precision.
        if len(data_precision_list) != len(data_list):
            data_precision_list = [None] * len(data_list)
        pxpyfactory.helpers.print_filter(f"  Updated data_precision_list: {data_precision_list}", 2)
        if len(data_units_list) != len(data_list):
            data_units_list = [None] * len(data_list)
        pxpyfactory.helpers.print_filter(f"  Updated data_units_list: {data_units_list}", 2)

        stub_list = _get_columns_from_meta(table_meta_cs, 'STUB') # Update stub_list based on content in spesific metadata if it exists
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

        heading_list = _get_columns_from_meta(table_meta_cs, 'HEADING') # Update heading_list based on content in spesific metadata if it exists
        pxpyfactory.helpers.print_filter(f"  Initial heading_list: {heading_list}", 2)
        if pxpyfactory.validation.is_list_empty(heading_list):
            heading_list = []
        # Add all remaining columns must be headings:
        for column in remaining_columns:
            if column not in heading_list:
                heading_list.append(column)
        pxpyfactory.helpers.print_filter(f"  Updated heading_list: {heading_list}", 2)

        timeval_must_be_in_headings_or_stubs = stub_list + heading_list
        for timeval_col in timeval_list:
            if timeval_col not in timeval_must_be_in_headings_or_stubs:
                pxpyfactory.helpers.print_filter(f"WARNING: TIMEVAL column '{timeval_col}' is not included in DATA columns. This column will not be treated as a time variable.", 1)
                timeval_list = []
        
        # Ensure correct formatting of content
        for column in table_data.columns:
            if column in data_list:
                # Ensure all decimal commas are replaced with dots in data columns
                table_data[column] = table_data[column].apply(lambda x: x.replace(',', '.') if isinstance(x, str) and ',' in x else x)
                table_data[column] = table_data[column].apply(lambda x: x.replace(' ', '_') if isinstance(x, str) and ' ' in x else x)
            else:
                # Ensure all values in non-data columns are strings (to avoid issues with mixed types):
                table_data[column] = table_data[column].apply(lambda x: str(x) if pd.notnull(x) else '')

        return table_data, stub_list, heading_list, data_list, data_precision_list, data_units_list, timeval_list

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
    # Update or add keyword values from _meta file (spesific for this data product - table_meta_px).
    # For each row in table_meta_px, the value and language are added to the corresponding keyword instance in keywords.
    # If the keyword from table_meta_px is not found in keywords, a warning is printed and the keyword is ignored.
    def _update_standalone_keywords(self, keywords, table_meta_px):
        for _, row in table_meta_px.iterrows():
            keyword = row['KEYWORD']
            language = row['LANGUAGE']
            value = row['VALUE']
            if keyword == 'CONTACT':
                value = pxpyfactory.keyword_contact.shape_to_px(value) # Ensure correct formatting of contact information.
            if keyword in keywords:
                if keywords[keyword].language_dependent and pxpyfactory.validation.valid_value(language):
                    keywords[keyword].set_value(value, language=language, value_none_to_empty_string=True) # Add value and language to the keyword instance
                else:
                    keywords[keyword].set_value(value, value_none_to_empty_string=True) # Add value to the keyword instance without language
            else:
                pxpyfactory.helpers.print_filter(f"WARNING: Keyword '{keyword}' from spesific metadata not found in metadata base. This keyword will be ignored.", 1)

        return keywords

    # _____________________________________________________________________________
    # Rename columns from table data based on values from _meta file (spesific for this data product - table_meta_cr).
    # Renaming of columns triggers updating several interconnected keywords.
    def _update_interconnected_keywords(self, keywords, table_meta_cr):
        # Order the rows in table_meta_cr to enshure that updates on spesified languages are done after updates on non-specified languages (all languages)
        table_meta_cr = table_meta_cr.sort_values(by=['LANGUAGE'], na_position='first')

        for _, row in table_meta_cr.iterrows():
            column = row['KEYWORD']
            language = row['LANGUAGE']
            value = row['VALUE']

            if not pxpyfactory.validation.valid_value(column) or not pxpyfactory.validation.valid_value(value):
                continue
            if not pxpyfactory.validation.valid_value(language):
                language = None

            # List of keywords that have interconnected values that need to be updated together
            # These keywords are interconnected because they all refer to the same column names and values in the data table,
            #   and changes in one of them should be reflected in the others to maintain consistency.
            # For example, if a column name is renamed, it should be updated in all these keywords
            #   to ensure that the correct values are associated with the correct columns in the generated .px file.
            keywords_interconnected = ['STUB', 'HEADING', 'CONTVARIABLE', 'VALUES', 'TIMEVAL', 'UNITS', 'PRECISION', 'LAST-UPDATED', 'CONTACT']

            for keyword in keywords_interconnected:
                if keywords[keyword] is None:
                    pxpyfactory.helpers.print_filter(f"WARNING: Keyword '{keyword}' is missing.", 1)
                    continue
                keywords[keyword].update_columns(column=column, value=value, language=language)

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
        
        table_meta_cs      = table_meta[table_meta['TYPE'] == 'CS'][['KEYWORD', 'VALUE']]      # create df with only cs parameters
        table_meta_px      = table_meta[table_meta['TYPE'] == 'PX'][['KEYWORD', 'LANGUAGE', 'VALUE']]      # create df with only px parameters
        table_meta_cr      = table_meta[table_meta['TYPE'] == 'CR'][['KEYWORD', 'LANGUAGE', 'VALUE']]      # create df with only cr parameters
        table_meta_sq      = table_meta[table_meta['TYPE'] == 'SQ'][['KEYWORD', 'VALUE']]      # create df with only sq parameters
        return table_meta_cs, table_meta_px, table_meta_cr, table_meta_sq
        


    # _____________________________________________________________________________
    # Raw keyword values from input table and metadata are set in keywords base. This is done before interconnected keywords are updated to ensure that there are values to update from.
    # Language for initial values taken directly from input table or metadata without translation
    def _set_keywords_base_for_data_product(self, language_initial='raw'):
        keywords = copy.deepcopy(self.main_app.keywords_base) # Detached copy of keywords_base and keywords in it; changes here must not affect main_app.keywords_base

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
        keywords['VALUES'].set_value(self.data_list, scope_name=self.contvariable, language=language_initial) 

        for key, value in self.values_dict.items():
            keywords['VALUES'].set_value(value, scope_name=key, language=language_initial)
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
                    keywords['TIMEVAL'].set_value(timeval_value, scope_name=key, language=language_initial)

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
            keywords['LAST-UPDATED'].set_value(None, scope_name=data_col, language=language_initial)
            keywords['CONTACT'].set_value(None, scope_name=data_col, language=language_initial)

            # If there is more data columns than units, use the first unit for the rest
            if self.data_units_list == []:
                units_value = ''
            elif index < len(self.data_units_list):
                units_value = self.data_units_list[index]
            else:
                units_value = self.data_units_list[0]

            keywords['UNITS'].set_value(units_value, scope_name=data_col, language=language_initial)

            try:
                data_precision = int(self.data_precision_list[index])
            except Exception:
                data_precision = None
            if data_precision is not None:
                keywords['PRECISION'].set_value(data_precision, scope_name=[self.contvariable, data_col], language=language_initial)
            
            # If needed, more data-column specific px-parameters can be added. Value can be based on 'spesific_value' and implementet a few lines lower.
            # keywords['STOCKFA'].set_value(value, scope_name=data_col, language=language_initial)
            # keywords['CFPRICES'].set_value(value, scope_name=data_col, language=language_initial)
            # keywords['DAYADJ'].set_value(value, scope_name=data_col, language=language_initial)
            # keywords['SEASADJ'].set_value(value, scope_name=data_col, language=language_initial)

        return keywords

    # _____________________________________________________________________________
    def _get_all_px_lines_from_keywords(self, keywords, languages, main_language):
        # Get lines for all keywords to be written to the .px file. This includes handling of language-specific values and warnings for missing mandatory values.
        keyword_output_lines = []
        
        sorted_keywords = sorted(keywords.items(), key=lambda item: item[1].order)

        for keyword_name, keyword in sorted_keywords:
            pxpyfactory.helpers.print_filter(f"Keyword: {keyword_name} - order: {keyword.order} - scope_refs: {keyword.scope_refs} - default_value: {keyword.default_value}", 4)
            if keyword.order < 1000: # Only include keywords with order < 1000 in the .px file; the rest are used for internal handling and not part of the final .px content
                keyword_lines = keyword.get_px_lines(languages=languages, main_language=main_language, warn_on_missing_mandatory=True)
                keyword_output_lines.extend(keyword_lines)

        return keyword_output_lines

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
    

