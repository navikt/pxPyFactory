import pandas as pd
from pathlib import Path
# import os
# import pprint

# _____________________________________________________________________________
# Reads content from Excel or CSV files and returns a DataFrame
# If the file cannot be read, it returns an empty DataFrame and prints an error message
def file_read(filepath, sheet_name='Ark1', sep=';', header=0):
    df = pd.DataFrame()
    try:
        if filepath.endswith('.xlsx'):
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=header)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath, sep=sep, header=header)
        else:
            raise ValueError("Unsupported file type")
        df.columns = [column.strip().upper().replace(" ", "_").replace("Æ", "AE").replace("Ø", "O").replace("Å", "AA") for column in df.columns]
        return df
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return df
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return df
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
def make_path(folder_dict, base_path):
    for level1, value1 in folder_dict.items():
        alias1 = value1.get('alias', '')
        level1_path = base_path / str(level1)
        try_write(alias1, level1_path)
        subfolders = value1.get('subfolder', {})
        for level2, value2 in subfolders.items():
            alias2 = value2.get('alias', '')
            level2_path = level1_path / str(level2)
            try_write(alias2, level2_path)
# _____________________________________________________________________________
# Save the metadata to a .px file
def try_write(alias, path):
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error creating folder {path}: {e}")
    try:
        with open(path / 'no.alias', 'w', encoding='utf-8') as f:
            f.write(str(alias))
    except Exception as e:
        print(f"Error writing alias file in {path}: {e}")

# _____________________________________________________________________________
# Save the metadata to a .px file
def write_px(list_of_lines, file_path):
    if file_path is None: # Print to console if no file path is given
        print('#'*80)
        for line in list_of_lines:
            print(line[:200])
        print('#'*80)
    else:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                for line in list_of_lines:
                    f.write(line + "\n")
        except Exception as e:
            print(f"Error writing PX file {file_path}: {e}")
