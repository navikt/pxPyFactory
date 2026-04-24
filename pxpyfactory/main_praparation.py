import pandas as pd
import pxpyfactory.keyword
import pxpyfactory.file_io
import pxpyfactory.helpers
import pxpyfactory.validation


# Get and prepare data products for PX file generation from Excel sheet.
def prepare_data_products(common_meta_filepath):
    data_products = pxpyfactory.file_io.file_read(common_meta_filepath, sheet_name='dataprodukter')
    data_products = data_products.map(lambda x: str(x) if pd.notnull(x) else None)

    data_products['TABLEID_RAW'] = data_products['TABLEID']
    data_products['TABLEID'] = data_products['TABLEID'].apply(pxpyfactory.helpers.shorten_tableid)

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

    # Remove data products where BUILD is not set to 'x' in Excel sheet and FORCE_BUILD is not True or None
    data_products = data_products[(data_products['BUILD'] == 'x') & (data_products['FORCE_BUILD'] != False)]

    duplicates_mask = data_products.duplicated(subset=['TABLEID'], keep='first')
    duplicates_df = data_products[duplicates_mask].copy()
    data_products = data_products[~duplicates_mask].copy()

    pxpyfactory.helpers.print_filter('Data products / tables to create px-files from:', 0)
    pxpyfactory.helpers.print_filter(data_products[['SUBJECT-CODE', 'SUBJECT-AREA', 'SUBJECT', 'TABLEID', 'TITLE']], 0)
    if duplicates_df.shape[0] > 0:
        pxpyfactory.helpers.print_filter('--- Duplicated table numbers (will be skipped):', 0)
        pxpyfactory.helpers.print_filter(duplicates_df, 0)

    return data_products


# Prepare alias folder names from Excel sheets - common for all data products.
def prepare_alias(common_meta_filepath):
    alias = pxpyfactory.file_io.file_read(common_meta_filepath, sheet_name='folder-alias')
    pxpyfactory.helpers.print_filter('Alias table:', 3)
    pxpyfactory.helpers.print_filter(alias, 3)
    alias = alias[['CODE', 'NO', 'EN']]
    alias['NO'] = alias['NO'].where(alias['NO'].apply(pxpyfactory.validation.valid_value), alias['EN'])
    alias['EN'] = alias['EN'].where(alias['EN'].apply(pxpyfactory.validation.valid_value), alias['NO'])
    alias = alias[alias['CODE'].apply(pxpyfactory.validation.valid_value) & alias['NO'].apply(pxpyfactory.validation.valid_value)]

    duplicates_mask = alias.duplicated(subset=['CODE'], keep='first')
    alias = alias[~duplicates_mask].copy()
    return alias


# Create the folder structure to put the PX files in.
def update_folder_structure(data_products_df, alias_df, output_path):
    path_list = []
    languages = ['no', 'en']
    for _, row in data_products_df.iterrows():
        level1_path = output_path + '/' + str(row['SUBJECT-AREA']).replace('/', '')
        level2_path = level1_path + '/' + str(row['SUBJECT']).replace('/', '')

        if level1_path not in path_list:
            path_list.append(level1_path)
        if level2_path not in path_list:
            path_list.append(level2_path)

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
