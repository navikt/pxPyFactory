import pandas as pd
from datetime import datetime

# _____________________________________________________________________________
# For each row in metadata_base, if the 'keyword' is found in amendment_df,
# insert the value from amendment_df to the new_column_name-column in base_df.
def metadata_add(base_df, amendment_df, new_column_name):
    base_df[new_column_name] = None
    for idx, row in base_df.iterrows():
        match = amendment_df[amendment_df['KEYWORD'] == row['KEYWORD']]
        if not match.empty:
            base_df.at[idx, new_column_name] = match.iloc[0]['VALUE']
#    print('Added value from other source:')
#    print(base_df[base_df[new_column_name].notnull()])
    return base_df
# _____________________________________________________________________________
# ...
def update_metadata(df, keyword, column, new_value, mandatory=True):
    mask = df['KEYWORD'] == keyword
    print(f"Updating SUBJECT_CODE to: {new_value}") if keyword == 'SUBJECT_CODE' else None
    if mask.sum() == 0: # The keyword does not exist, so we add a new row
        new_row = pd.Series({'KEYWORD': keyword, column: new_value, 'MANDATORY': mandatory, 'TYPE': 'text', 'ORDER': 900}).reindex(df.columns)
        df = pd.concat([df, new_row.to_frame().T], ignore_index=True)
    elif mask.sum() == 1: # Only one entry found for the keyword, so we update it
        df.at[df.index[mask][0], column] = new_value
        df.at[df.index[mask][0], 'MANDATORY'] = mandatory
    else: # The keyword exists multiple times -> Raise an error
        raise ValueError(f"Multiple entries found for keyword: {', '.join(df['KEYWORD'].tolist())}")
    # print(f"# Updated metadata for keyword '{keyword}':")
    # print(df[df['KEYWORD'] == keyword])
    return df
# _____________________________________________________________________________
# Prepare the lines that will be written to the .px file
def prepare_px_lines(meta_df, data_df):
    list_of_lines_to_px = []
    fill_item = "."
    for _, meta_row in meta_df.iterrows():
        row_keyword = meta_row['KEYWORD']
        row_value = meta_row['VALUE']
        row_type = meta_row['TYPE']
        if row_type == 'integer':
            value_out = row_value
        elif row_type == 'integer/text': # This is for the DATA part
            value_out = ''
            for i, row in enumerate(data_df.itertuples(index=False, name=None)):
                value_out += '\n'
                formatted_row = [f'"{fill_item}"' if pd.isnull(item) else item for item in row]
                value_out += ' '.join(map(str, formatted_row))
        # If none of the above, assume text
        elif isinstance(row_value, str):
            # print(f"Preparing for .px: {row_keyword}={row_value}, type desc={row_type}, type of current value={type(row_value)})")
            value_out = '"' + row_value + '"'
        elif isinstance(row_value, list):
            if all(isinstance(x, str) for x in row_value):
                value_out = '"' + '", "'.join(row_value) + '"'
            elif any(isinstance(x, str) for x in row_value):
                value_out = '"' + '", "'.join([str(x) for x in row_value]) + '"'
            else: # all(isinstance(x, (int, float)) for x in row_value):
                value_out = ', '.join(str(x) for x in row_value)
        else:
            print(f"Unclear type for: keyword={row_keyword}, value={row_value}, type desc={row_type}, type of current value={type(row_value)})")
            value_out = '"' + str(row_value) + '"'
        # print(f"Prepared for .px: {row_keyword}={value_out}, mandatory={meta_row['MANDATORY']}") if row_keyword != 'DATA' else None
        list_of_lines_to_px.append(f'{row_keyword}={value_out};')
    return list_of_lines_to_px

# _____________________________________________________________________________
# Returns the first non-null value in a row. If the value is a list or tuple,
# it checks if it has exactly one element and that element is null, in which case it skips it.
# If all values are null, it returns an empty string.
def get_first_notnull(row):
    for value in row:
        # # If value is a list, tuple, or np.ndarray
        # if isinstance(value, (list, tuple)): #, np.ndarray)):
        #     # If it has exactly one element and that element is null, skip it
        #     if len(value) == 0 or (len(value) == 1 and pd.isnull(value[0])):
        #         continue
        #     else:
        #         return value
        # # If value is a scalar and not null, return it
        # elif pd.notnull(value):
        #     return value
        if valid_value(value):
            return value
        else:
            continue
    return ''
# _____________________________________________________________________________
def valid_value(value):
    # If value is a list, tuple, or np.ndarray
    if isinstance(value, (list, tuple, pd.Series)): #, np.ndarray)):
        # If it has exactly one element and that element is null, skip it
        if len(value) == 0 or (len(value) == 1 and pd.isnull(value[0])):
            return False
        else:
            return True
    # If value is a scalar and not null, return it
    elif value == '':
        return False
    elif pd.notnull(value):
        return True
    else:
        return False
# _____________________________________________________________________________
def alert_missing_mandatory(df):
    mandatory_and_missing = df['KEYWORD'][df['MANDATORY'] & (df.apply(lambda row: valid_value(row['VALUE']), axis=1) == False)].tolist()
    try:
        mandatory_and_missing.remove('DATA')
    except ValueError:
        pass  # Do nothing if the string is not in the list
    print(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}") if len(mandatory_and_missing) > 0 else None
    # raise ValueError(f"Missing mandatory keywords: {', '.join(mandatory_and_missing)}")

# _____________________________________________________________________________


"""
# _____________________________________________________________________________
def metadata_clean(metadata_df):

#    # Copy value in defaut_value to value if value is None and default_value is not None
#    metadata_df['value'] = metadata_df.apply(
#        lambda row: row['default_value'] if pd.isna(row['value']) and pd.notna(row['default_value']) else row['value'], axis=1
#    )

    # Remove rows with keywords that are not mandatory, and dont contain any value
    metadata_df = metadata_df[metadata_df['mandatory'] | metadata_df['value'].notna()] # Both None and NaN are included in .notna().

    # Raise an error if any mandatory keyword is missing
    mandatory_and_missing = metadata_df[metadata_df['mandatory'] & metadata_df['value'].isna()]
    if not mandatory_and_missing.empty:
        missing_keywords = mandatory_and_missing['keyword'].tolist()
        raise ValueError(f"Missing mandatory keywords: {', '.join(missing_keywords)}")

    return metadata_df
# _____________________________________________________________________________
def prepare_content(df: pandas.DataFrame, stub: list, headings: list, contvariable: str, px_path: str) -> None:
    # data dimentions
    y_dim = 1
    x_dim = 1
    values_dict = {}
    breakpoint()
    # Process stubs
    for column in stub:
        stub_values = list(df[column].unique())
        values_dict[column] = stub_values
        x_dim *= len(stub_values)
    
    # Process headings
    for column in headings:
        heading_values = list(df[column].unique())
        values_dict[column] = heading_values
        y_dim *= len(heading_values)

    df.sort_values(by = stub + headings, inplace=True)

    # Ensure all combinations of column values are present in the table
    complete_df = df.pivot_table(
        index=stub,              # Stub columns (vertical axis)
        columns=headings,        # Heading columns (horizontal axis)
        values='PX_DATA',     # Values to populate the table
        fill_value='".."'          # Fill missing combinations with NaN
    )
    # Flatten the pivoted data to prepare for writing to a .px file
    data_array = complete_df.to_numpy()


# _____________________________________________________________________________
def add_lines_to_content(content: list = None , lines: list) -> list:
    if content is None: content = []

    content.append(f'STUB={",".join(f"\"{s}\"" for s in stub)};')
    content.append(f'HEADING={",".join(f"\"{h}\"" for h in headings)};')
    content.append(f'CONTVARIABLE="{contvariable}";')
    content.append('') # Adds a blank line for better readability

    # Write VALUES for each column
    for key, value in values_dict.items():
        values = ",".join(f'"{v}"' for v in value)
        content.append(f'VALUES("{key}")={values};')
    content.append('')

    # Write DATA
    content.append("DATA=")
    for i, row in enumerate(data_array):
        content.append(" ".join(map(str, row)))
    content.append(";") # End the data with a semicolon
    content.append('')

    return content

# _____________________________________________________________________________
# Returns the file path for where the .px file will be stored
def get_file_path(df) -> str:
    directory = df.get("DIRECTORY-PATH", None)
    stem = df.get("MATRIX", None)
    suffix = '.px'
    return f"{directory}/{stem}{suffix}"

# _____________________________________________________________________________
# Writes the content to a file. Each item in the input list is written as a new line in the file.
def write_to_file(content:list, file_path: str) -> None:
    with open(file_path, 'w', encoding='windows-1252') as f: # utf-8
        for line in content:
            f.write(line + '\n')

# _____________________________________________________________________________

# _____________________________________________________________________________
def metadata_init():
    current_time = datetime.now().strftime("%Y%m%d %H:%M")
    metadata_list = [
        {"order":  1, "keyword": "CHARSET"              , "mandatory": False , "default_value": "ANSI"       , "language_dependent": False , "type": "text"        , "explanation": "Indicates text encoding, e.g. ANSI."},
        {"order":  2, "keyword": "AXIS-VERSION"         , "mandatory": False , "default_value": "2010"       , "language_dependent": False , "type": "text"        , "explanation": "Version number for PC-Axis."},
        {"order":  3, "keyword": "CODEPAGE"             , "mandatory": False , "default_value": "iso-8859-1" , "language_dependent": False , "type": "text"        , "explanation": "Used for XML format to get correct characters."},
        {"order":  4, "keyword": "LANGUAGE"             , "mandatory": False , "default_value": "no"         , "language_dependent": True  , "type": "text"        , "explanation": "The language used in the PC-Axis file (2 chars), sv for Swedish, en for English etc. Compare language codes for text files. If the keyword is used, the words for “and” and “by” are read from the text file of that language. Otherwise these worrds are read from the text file of the language in which the program is running."},
        {"order":  5, "keyword": "LANGUAGES"            , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "If more than one language in the PC-Axis file then all languages are mentioned here (2 chars each)"},
        {"order":  6, "keyword": "CREATION-DATE"        , "mandatory": False , "default_value": current_time , "language_dependent": False , "type": "text"        , "explanation": "Date when file was created. Format CCYYMMDD hh:mm."},
        {"order":  7, "keyword": "NEXT-UPDATE"          , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Next update for the table."},
        {"order":  8, "keyword": "PX-SERVER"            , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "PX server for the table."},
        {"order":  9, "keyword": "DIRECTORY-PATH"       , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Directory path for the table."},
        {"order": 10, "keyword": "UPDATE-FREQUENCY"     , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Update frequency for the table."},
        {"order": 11, "keyword": "TABLEID"              , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Table identifier."},
        {"order": 12, "keyword": "SYNONYMS"             , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Synonyms for the table."},
        {"order": 13, "keyword": "DEFAULT-GRAPH"        , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Default graph for the table."},
        {"order": 14, "keyword": "DECIMALS"             , "mandatory": True  , "default_value": 1            , "language_dependent": False , "type": "integer"     , "explanation": "Number of decimals in the table cells."},
        {"order": 15, "keyword": "SHOWDECIMALS"         , "mandatory": False , "default_value": 0            , "language_dependent": False , "type": "integer"     , "explanation": "Indicates how many decimals will be shown."},
        {"order": 16, "keyword": "ROUNDING"             , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "integer"     , "explanation": "Rounding for the table."},
        {"order": 17, "keyword": "MATRIX"               , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "The name of the matrix. Is suggested as file name when the file is fetched."},
        {"order": 18, "keyword": "AGGREGALLOWED"        , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Allows aggregation of values."},
        {"order": 19, "keyword": "AUTOPEN"              , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Removes the window 'Select variables and values' in PC-Axis when file is downloaded."},
        {"order": 20, "keyword": "SUBJECT-CODE"         , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Subject area code for the table."},
        {"order": 21, "keyword": "SUBJECT-AREA"         , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "The name of the subject area in plain text."},
        {"order": 22, "keyword": "CONFIDENTIAL"         , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "integer"     , "explanation": "Possibility to do some manipulation with the data in the data part of the file."},
        {"order": 23, "keyword": "COPYRIGHT"            , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Copyright is given as YES or NO."},
        {"order": 24, "keyword": "DESCRIPTION"          , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Description of the table, used for display in menus."},
        {"order": 25, "keyword": "TITLE"                , "mandatory": True  , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "The title of the table, reflecting its contents and variables."},
        {"order": 26, "keyword": "DESCRIPTIONDEFAULT"   , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Description shown instead of the title."},
        {"order": 27, "keyword": "CONTENTS"             , "mandatory": True  , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Information about the contents, which makes up the first part of a title."},
        {"order": 28, "keyword": "UNITS"                , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Unit text, e.g. ton, index."},
        {"order": 29, "keyword": "STUB"                 , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Variables for the stub (rows) of the table."},
        {"order": 30, "keyword": "HEADING"              , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Variables for the heading (columns) of the table."},
        {"order": 31, "keyword": "CONTVARIABLE"         , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates that the table has two or more different contents."},
#        {"order": 32, "keyword": "VALUES"              , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "List of values for each variable."},
# Can have multiple entries, one for each variable.                                                                                                                                                                               
#   Also VARABLECODE..                                                                                                                                                                                                            
        {"order": 33, "keyword": "TIMEVAL"              , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Time values for the time variable."},
        {"order": 34, "keyword": "CODES"                , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Codes for variables if both code and plain text exist."},
# Can have multiple entries, one for each variable.                                                                                                                                                                               
        {"order": 35, "keyword": "DOUBLECOLUMN"         , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates if double column is used."},
        {"order": 36, "keyword": "PRESTEXT"             , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Presentation text for the table."},
        {"order": 37, "keyword": "DOMAIN"               , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Domain for the table."},
        {"order": 38, "keyword": "VARIABLE-TYPE"        , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Type of variable (e.g. time, value, etc.)."},
        {"order": 39, "keyword": "HIERARCHIES"          , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Hierarchies for the table."},
        {"order": 40, "keyword": "HIERARCHYLEVELS"      , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Hierarchy levels for the table."},
        {"order": 41, "keyword": "HIERARCHYLEVELSOPEN"  , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Open hierarchy levels for the table."},
        {"order": 42, "keyword": "HIERARCHYNAMES"       , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Hierarchy names for the table."},
        {"order": 43, "keyword": "MAP"                  , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Map for the table."},
        {"order": 44, "keyword": "PARTITIONED"          , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Partitioned for the table."},
        {"order": 45, "keyword": "ELIMINATION"          , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates if elimination is used."},
        {"order": 46, "keyword": "PRECISION"            , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "integer"     , "explanation": "Precision for the table."},
        {"order": 47, "keyword": "LAST-UPDATED"         , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Date when the contents were last updated."},
        {"order": 48, "keyword": "STOCKFA"              , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates if the contents are stock or flow."},
        {"order": 49, "keyword": "CFPRICES"             , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates if data is in current or fixed prices."},
        {"order": 50, "keyword": "DAYADJ"               , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates if the data is day adjusted."},
        {"order": 51, "keyword": "SEASADJ"              , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates if the data is seasonally adjusted."},
        # {"order": 52, "keyword": "UNITS"              , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Unit text, e.g. ton, index."},
        {"order": 53, "keyword": "CONTACT"              , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Contact information for the statistics."},
        {"order": 54, "keyword": "REFPERIOD"            , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Reference period for the contents."},
        {"order": 55, "keyword": "BASEPERIOD"           , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Base period for, for instance index series."},
        {"order": 56, "keyword": "DATABASE"             , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Name of the database from where the statistics is retrieved."},
        {"order": 57, "keyword": "SOURCE"               , "mandatory": False , "default_value": "Nav"        , "language_dependent": True  , "type": "text"        , "explanation": "Source of the statistics."},
        {"order": 58, "keyword": "SURVEY"               , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Survey for the table."},
        {"order": 59, "keyword": "LINK"                 , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Link for the table."},
        {"order": 60, "keyword": "INFOFILE"             , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Information file for the table."},
        {"order": 61, "keyword": "FIRST-PUBLISHED"      , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Date when the table was first published."},
        {"order": 62, "keyword": "META-ID"              , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Meta ID for the table."},
        {"order": 63, "keyword": "OFFICIAL-STATISTICS"  , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Indicates if the table is official statistics."},
        {"order": 64, "keyword": "INFO"                 , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Info for the table."},
        {"order": 65, "keyword": "NOTEX"                , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Extended note for the table."},
        {"order": 66, "keyword": "NOTE"                 , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Note for the table."},
        {"order": 67, "keyword": "VALUENOTEX"           , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Extended value note for the table."},
        {"order": 68, "keyword": "VALUENOTE"            , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Value note for the table."},
        {"order": 69, "keyword": "CELLNOTEX"            , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "As CELLNOTE but shown mandatory as for NOTEX."},
        {"order": 70, "keyword": "CELLNOTE"             , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Footnote for a single cell or a group of cells."},
                                                                                                                                                                                                                                  
        {"order": 71, "keyword": "DATASYMBOL1"          , "mandatory": False , "default_value": "."          , "language_dependent": False , "type": "text"        , "explanation": "Symbol for missing data (one dot)."},
        {"order": 72, "keyword": "DATASYMBOL2"          , "mandatory": False , "default_value": ".."         , "language_dependent": False , "type": "text"        , "explanation": "Symbol for missing data (two dots)."},
        {"order": 73, "keyword": "DATASYMBOL3"          , "mandatory": False , "default_value": "..."        , "language_dependent": False , "type": "text"        , "explanation": "Symbol for missing data (three dots)."},
        {"order": 74, "keyword": "DATASYMBOL4"          , "mandatory": False , "default_value": "...."       , "language_dependent": False , "type": "text"        , "explanation": "Symbol for missing data (four dots)."},
        {"order": 75, "keyword": "DATASYMBOL5"          , "mandatory": False , "default_value": "....."      , "language_dependent": False , "type": "text"        , "explanation": "Symbol for missing data (five dots)."},
        {"order": 76, "keyword": "DATASYMBOL6"          , "mandatory": False , "default_value": "......"     , "language_dependent": False , "type": "text"        , "explanation": "Symbol for missing data (six dots)."},
        {"order": 77, "keyword": "DATASYMBOLSUM"        , "mandatory": False , "default_value": "*"          , "language_dependent": False , "type": "text"        , "explanation": "Symbol for sum of differing datanote symbols."},
        {"order": 78, "keyword": "DATASYMBOLNIL"        , "mandatory": False , "default_value": "-"          , "language_dependent": False , "type": "text"        , "explanation": "Symbol for nil data (dash)."},
        {"order": 79, "keyword": "DATANOTECELL"         , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Note for a specific cell, indicated by codes for each variable."},
        {"order": 80, "keyword": "DATANOTESUM"          , "mandatory": False , "default_value": "*"          , "language_dependent": False , "type": "text"        , "explanation": "Note for sum of differing datanote symbols."},
        {"order": 81, "keyword": "DATANOTE"             , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Indicates that a note exists for a certain element of the statistical cube."},
                                                                                                                                                                                                                                  
        {"order": 82, "keyword": "KEYS"                 , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Keys for the table."},
        {"order": 83, "keyword": "ATTRIBUTE-ID"         , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Lists the identities of all attributes."},
        {"order": 84, "keyword": "ATTRIBUTE-TEXT"       , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "text"        , "explanation": "Textual representation of the attribute for presentational purpose."},
        {"order": 85, "keyword": "ATTRIBUTES"           , "mandatory": False , "default_value": None         , "language_dependent": False , "type": "text"        , "explanation": "Specifies all attributes for a single observation value."},
        {"order": 86, "keyword": "PRECISION"            , "mandatory": False , "default_value": None         , "language_dependent": True  , "type": "integer"     , "explanation": "Precision for the table."},
                                                                                                                                                                                                                             
        {"order": 1000, "keyword": "DATA"               , "mandatory": True  , "default_value": None         , "language_dependent": False , "type": "integer/text", "explanation": "The actual data values for the table."}
    ]
    # Create dataframe from list of dict, and add column for key and value.
    metadata_df = pd.DataFrame(metadata_list)
    
    return metadata_df


"""