import pandas as pd
import pxpyfactory.keyword
import pxpyfactory.file_io
import pxpyfactory.helpers
import pxpyfactory.validation


# Get and prepare data products for PX file generation from Excel sheet.
def prepare_data_products(common_meta_filepath, input_path):
    source_arg = pxpyfactory.helpers.get_input_args('source')
    if (source_arg is None) or (source_arg == 'excel'):
        data_products = pxpyfactory.file_io.file_read(common_meta_filepath, sheet_name='dataprodukter')
        # Remove data products where BUILD is not set to 'x' in Excel sheet
        if ('TABLEID' in data_products.columns) and ('BUILD' in data_products.columns):
            # If there is a TABLEID with value '*' and BUILD value 'x', it means that all files in input-folder should be added as data products.
            if ((data_products['TABLEID'].astype(str).str.strip() == '*') & (data_products['BUILD'].astype(str).str.strip().str.lower() == 'x')).any():
                pxpyfactory.helpers.print_filter("Found TABLEID with value '*'. All files in input-folder will be added.", 1)
                data_products = data_products[data_products['TABLEID'] != '*']
                source_arg = 'folder'
            data_products = data_products[data_products['TABLEID'].apply(pxpyfactory.validation.valid_value) & (data_products['BUILD'].astype(str).str.strip().str.lower() == 'x')]
        else:
            data_products = pd.DataFrame()
        # Remove duplicated data products in Excel sheet
        duplicates_mask = data_products.duplicated(subset=['TABLEID'], keep='first')
        print(" Duplicates mask:")
        print(duplicates_mask)
        duplicates_df = data_products[duplicates_mask].copy()
        if not duplicates_df.empty:
            data_products = data_products[~duplicates_mask].copy()
            pxpyfactory.helpers.print_filter('--- Duplicated table numbers (will be skipped):', 0)
            pxpyfactory.helpers.print_filter(duplicates_df, 0)
    else:
        data_products = pd.DataFrame()

    # add missing columns if not present in Excel sheet to avoid errors later (will be filled with None values)
    expected_columns = ['BUILD', 'TABLEID', 'SUBJECT-CODE', 'SUBJECT-AREA', 'SUBJECT', 'TITLE', 'CONTENTS']
    for column in expected_columns:
        if column not in data_products.columns:
            data_products[column] = None
    data_products = data_products.map(lambda x: str(x) if pd.notnull(x) else None)  # Convert all cell-values to string, set None for NaN


    add_all_files_in_folder = False
    if source_arg == 'folder':
        pxpyfactory.helpers.print_filter(f"Source argument set to 'folder'. All files in input-folder will be added.", 1)
        add_all_files_in_folder = True
    elif data_products.empty:
        pxpyfactory.helpers.print_filter(f"File {common_meta_filepath} with sheet 'dataprodukter' not used. All files in input-folder will be added.", 1)
        add_all_files_in_folder = True

    if add_all_files_in_folder:
        # Implementation for adding all data product files in the folder
        files_in_folder = pxpyfactory.file_io.list_files_in_path(input_path)
        new_row = dict.fromkeys(data_products.columns, None)
        existing_tableids = {str(tableid).lower() for tableid in data_products['TABLEID'].dropna().values}
        for file in files_in_folder:
            # split file str on '.'
            file_parts = file.rsplit('.', 1)
            if len(file_parts) != 2:
                continue
            if file_parts[1].lower() in ['csv']: # , 'parquet']: # Only consider csv and parquet files as data products
                if (file_parts[0].lower().endswith('_meta') == False) and (file_parts[0].lower() not in existing_tableids):
                    new_row['TABLEID'] = file_parts[0]
                    data_products.loc[len(data_products)] = new_row
                    existing_tableids.add(file_parts[0].lower())

    # Filter based on command line arguments
    force_build_arg = pxpyfactory.helpers.get_input_args('build')
    if force_build_arg is None:
        data_products['FORCE_BUILD'] = None
    elif force_build_arg == 'all':
        data_products['FORCE_BUILD'] = True
    else:
        data_products['FORCE_BUILD'] = False
        data_products.loc[data_products['TABLEID'] == force_build_arg, 'FORCE_BUILD'] = True
        data_products.loc[data_products['TABLEID_RAW'] == force_build_arg, 'FORCE_BUILD'] = True
    # Remove data products where FORCE_BUILD is not True or None
    data_products = data_products[data_products['FORCE_BUILD'] != False]

    data_products['TABLEID_RAW'] = data_products['TABLEID']
    data_products['TABLEID'] = data_products['TABLEID'].apply(pxpyfactory.helpers.shorten_tableid)

    pxpyfactory.helpers.print_filter('Data products / tables to create px-files from:', 0)
    pxpyfactory.helpers.print_filter(data_products[['SUBJECT-CODE', 'SUBJECT-AREA', 'SUBJECT', 'TABLEID', 'TITLE']], 0)

    return data_products


# Prepare alias folder names from Excel sheets - common for all data products.
def prepare_alias(common_meta_filepath, language_preference_order):
    alias = pxpyfactory.file_io.file_read(common_meta_filepath, sheet_name='folder-alias')
    pxpyfactory.helpers.print_filter('Alias table:', 4)
    pxpyfactory.helpers.print_filter(alias, 4)

    if not {'CODE'}.issubset(alias.columns):
        pxpyfactory.helpers.print_filter("Missing or unvalid file for spesific metadata.", 4)
        return pd.DataFrame()
    valid_columns = ['CODE']
    for column in alias.columns:
        # append column name if it is a two-letter language code (e.g. 'NO', 'EN') to valid_columns list
        if len(column) == 2 and column.isalpha():
            valid_columns.append(column)
    alias = alias[valid_columns]
    lang_cols = [col for col in alias.columns if col != 'CODE']

    # Remove rows with no valid CODE or no valid value in any language column
    has_valid_lang = alias[lang_cols].apply(lambda row: row.apply(pxpyfactory.validation.valid_value).any(), axis=1)
    alias = alias[alias['CODE'].apply(pxpyfactory.validation.valid_value) & has_valid_lang].copy()

    # Merge rows with same CODE: use first valid value per language column
    def first_valid(series):
        valid_vals = series[series.apply(pxpyfactory.validation.valid_value)]
        return valid_vals.iloc[0] if not valid_vals.empty else None

    alias = alias.groupby('CODE', sort=False).agg({col: first_valid for col in lang_cols}).reset_index()

    # Fill missing language values: try each language in preference order, then fall back to CODE
    available_langs = [lang.upper() for lang in language_preference_order if lang.upper() in alias.columns]
    for language_col in available_langs:
        for fallback_lang in available_langs:
            alias[language_col] = alias[language_col].where(alias[language_col].apply(pxpyfactory.validation.valid_value), alias[fallback_lang])
        alias[language_col] = alias[language_col].where(alias[language_col].apply(pxpyfactory.validation.valid_value), alias['CODE'])

    return alias


# Create the folder structure to put the PX files in.
def update_folder_structure(data_products_df, alias_df, output_path, language_preference_order):
    # path_list = []
    # for _, row in data_products_df.iterrows():
    #     subject_area = row['SUBJECT-AREA']
    #     if subject_area is None:
    #         continue
    #     level1_path = output_path + '/' + str(subject_area).replace('/', '')
    #     if level1_path not in path_list:
    #         path_list.append(level1_path)

    #     subject = row['SUBJECT']
    #     if subject is None:
    #         continue
    #     level2_path = level1_path + '/' + str(subject).replace('/', '')
    #     if level2_path not in path_list:
    #         path_list.append(level2_path)
    files_in_path = pxpyfactory.file_io.list_files_in_path(output_path)
    folders_in_path = []
    for file in files_in_path:
        folder = file.rsplit('/', 1)[0]
        subfolders = folder.split('/')
        for i in range(len(subfolders)):
            folder = '/'.join(subfolders[:i+1])
            if folder not in folders_in_path:
                folders_in_path.append(folder)
    path_list = [output_path + '/' + folder for folder in folders_in_path]
    languages = [language.lower() for language in language_preference_order]
    for path in path_list:
        leaf = path.rsplit('/', 1)[-1]
        for language in languages:
            alias_value = leaf
            language_col = language.upper()
            if (leaf in alias_df['CODE'].values) and (language_col in alias_df.columns):
                alias_value = alias_df.loc[alias_df['CODE'] == leaf, language_col].iloc[0]
            file_path = path + '/' + 'alias_' + language + '.txt'
            pxpyfactory.file_io.file_write(file_path, alias_value)


# Prepare general metadata from Excel sheets - common for all data products.
def prepare_keywords_base(common_meta_filepath):
    meta_default = pxpyfactory.file_io.file_read(common_meta_filepath, sheet_name='metadata-default')
    meta_default = meta_default[['ORDER', 'KEYWORD', 'MANDATORY', 'LANGUAGE_DEPENDENT', 'DEFAULT_VALUE', 'TYPE', 'LENGTH', 'MULTILINE']]
    meta_default = meta_default[meta_default['KEYWORD'].apply(pxpyfactory.validation.valid_value)]  # Remove rows with invalid or missing keyword values

    keywords_base = {}
    for _, row in meta_default.iterrows():
        keyword = row['KEYWORD']
        order = row['ORDER']
        mandatory = row['MANDATORY']
        language_dependent = row['LANGUAGE_DEPENDENT']
        default_value = row['DEFAULT_VALUE']
        value_type = row['TYPE']
        length = row['LENGTH']
        multiline = row['MULTILINE']

        if keyword in keywords_base:
            pxpyfactory.helpers.print_filter(f"WARNING: Duplicate keyword in metadata-default: {keyword}. Keeping first occurrence.", 1)
            continue

        keyword_class = pxpyfactory.keyword.Keyword

        keywords_base[keyword] = keyword_class(
            name=keyword,
            order=order,
            mandatory=mandatory,
            language_dependent=language_dependent,
            default_value=default_value,
            value_type=value_type,
            length=length,
            multiline=multiline,
        )

        pxpyfactory.helpers.print_filter(
            f"Keyword: {keyword} - created with default_value: {default_value} (type: {type(default_value)} ), while value_type: {value_type}",
            4,
        )

    return keywords_base
