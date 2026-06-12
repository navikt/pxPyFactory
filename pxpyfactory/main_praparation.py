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
            data_products = data_products[pxpyfactory.helpers.validvalue(data_products['TABLEID']) & (data_products['BUILD'].astype(str).str.strip().str.lower() == 'x')]
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
        for file in files_in_folder:
            # split file str on '.'
            file_parts = file.rsplit('.', 1)
            if file_parts[1] in ['csv']: # , 'parquet']: # Only consider csv and parquet files as data products
                if (file_parts[0].endswith('_meta') == False) and (file_parts[0] not in data_products['TABLEID'].values):
                    new_row['TABLEID'] = file_parts[0]
                    data_products.loc[len(data_products)] = new_row

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
def prepare_alias(common_meta_filepath):
    alias = pxpyfactory.file_io.file_read(common_meta_filepath, sheet_name='folder-alias')
    pxpyfactory.helpers.print_filter('Alias table:', 4)
    pxpyfactory.helpers.print_filter(alias, 4)
    alias = alias[['CODE', 'NO', 'EN']]
    alias['NO'] = alias['NO'].where(alias['NO'].apply(pxpyfactory.validation.valid_value), alias['EN'])
    alias['EN'] = alias['EN'].where(alias['EN'].apply(pxpyfactory.validation.valid_value), alias['NO'])
    alias = alias[alias['CODE'].apply(pxpyfactory.validation.valid_value) & alias['NO'].apply(pxpyfactory.validation.valid_value)]

    duplicates_mask = alias.duplicated(subset=['CODE'], keep='first')
    alias = alias[~duplicates_mask].copy()
    return alias


# Create the folder structure to put the PX files in.
def update_folder_structure(data_products_df, alias_df, output_path):
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
    languages = ['no', 'en']
    for path in path_list:
        leaf = path.rsplit('/', 1)[-1]
        for language in languages:
            alias_value = leaf
            if leaf in alias_df['CODE'].values:
                alias_value = alias_df.loc[alias_df['CODE'] == leaf, language.upper()].iloc[0]
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
