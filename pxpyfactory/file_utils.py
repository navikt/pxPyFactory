import pandas as pd
import pprint
from pathlib import Path

# _____________________________________________________________________________
def file_read(filepath, sheet_name='Ark1', sep=';', header=0):
    # Determins the type of the file, and puts the content and returns a DataFrame.
    if filepath.endswith('.xlsx'):
        df = pd.read_excel(filepath, sheet_name=sheet_name, header=header)
    elif filepath.endswith('.csv'):
        df = pd.read_csv(filepath, sep=sep, header=header) #, encoding='latin1')
    df.columns = [column.strip().upper().replace(" ", "_") for column in df.columns] # Standardize column names
    return df
    # try:
    #     table_meta = file_read(f"TN_{row['tabellnummer']}_meta.csv")
    # except Exception:
    #     table_meta = pd.DataFrame(columns=['keyword', 'value'])

# _____________________________________________________________________________
# Create folder structure from data_products dataframe
# Create the folder structure to put the px-files in
def update_folder_structure(df, folderpath):
    base_path = Path(folderpath)
    folder_dict = get_folder_structure(df)
    # read_folder_structure_from_storage(base_path)
    make_path(folder_dict, base_path)

def read_folder_structure_from_storage(base_path):
    for base_path in root.rglob("*"):
        if base_path.is_file():
            print(f"File: {base_path}")
        elif base_path.is_dir():
            print(f"Folder: {base_path}")

def make_path(folder_dict, base_path):
    for level1, value1 in folder_dict.items():
        level1_path = base_path / str(level1)
        level1_path.mkdir(parents=True, exist_ok=True)
        # Write alias file in level 1 folder
        alias1 = value1.get('alias', '')
        with open(level1_path / 'no.alias', 'w', encoding='utf-8') as f:
            f.write(str(alias1))
        subfolders = value1.get('subfolder', {})
        for level2, value2 in subfolders.items():
            level2_path = level1_path / str(level2)
            level2_path.mkdir(parents=True, exist_ok=True)
            # Write alias file in level 2 folder
            alias2 = value2.get('alias', '')
            with open(level2_path / 'no.alias', 'w', encoding='utf-8') as f:
                f.write(str(alias2))
# _____________________________________________________________________________
# Read folder structure from data_products dataframe
def get_folder_structure(df):
    # Create a list of level 1 folders:
    # level_1_folders = data_products['LEVEL_1'].unique().tolist()
    folder_dict = {}

    for _, row in df.iterrows():
        level1 = row['LEVEL_1']
        level1_no = row['LEVEL_1_(NO)']
        level2 = row['LEVEL_2']
        level2_no = row['LEVEL_2_(NO)']

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
    pprint.pp(folder_dict)
    # print_folder_dict(folder_dict)
    return folder_dict

# _____________________________________________________________________________
# Save the metadata to a .px file
def write_px(list_of_lines, file_path):
    if file_path is None: # Print to console if no file path is given
        print('#'*80)
        for line in list_of_lines:
            print(line[:200])
        print('#'*80)
    else:
        with open(file_path, "w", encoding="utf-8") as f:
            for line in list_of_lines:
                f.write(line + "\n")
