from pathlib import Path
from pxpyfactory.io_utils import write_folder_alias

# _____________________________________________________________________________
# Create folder structure from data_products dataframe
# Create the folder structure to put the px-files in
def update_folder_structure(df, folderpath):
    base_path = Path(folderpath)
    folder_dict = find_path(df)
    # read_folder_structure_from_storage(base_path)
    make_path(folder_dict, base_path)
# _____________________________________________________________________________
# Read folder structure from data_products dataframe
def find_path(df):
    folder_dict = {}

    for _, row in df.iterrows():
        level1 = row['LEVEL_1_FOLDER']
        level1_no = row['LEVEL_1']
        level2 = row['LEVEL_2_FOLDER']
        level2_no = row['LEVEL_2']

        # Add level 1 if not already present
        if level1 not in folder_dict:
            folder_dict[level1] = {
                'alias': level1_no,
                'subfolder': {}
            }
        # Add subfolder if not already present under this folder
        if level2 not in folder_dict[level1]['subfolder']:
            folder_dict[level1]['subfolder'][level2] = {
                'alias': level2_no
            }
    # pprint.pp(folder_dict)
    return folder_dict
# _____________________________________________________________________________
# Create the folders and write alias files in them
def make_path(folder_dict, base_path, language=None):
    for level1, value1 in folder_dict.items():
        alias1 = value1.get('alias', '')
        level1_path = base_path / str(level1)
        level1_file_path = level1_path / ('alias_' + language + '.txt' if language else 'alias.txt')
        write_folder_alias(alias1, level1_path, level1_file_path)
        subfolders = value1.get('subfolder', {})

        for level2, value2 in subfolders.items():
            alias2 = value2.get('alias', '')
            level2_path = level1_path / str(level2)
            level2_file_path = level2_path / ('alias_' + language + '.txt' if language else 'alias.txt')
            write_folder_alias(alias2, level2_path, level2_file_path)
