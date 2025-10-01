import pandas as pd
# import pprint
import os

def set_script_path():
    # Set folders and file-paths
    script_path = os.path.dirname(os.path.abspath(__file__)) # Get path for this script location
    os.chdir(script_path) # Set working directory
    return script_path

def get_path(*path_parts):
    return os.path.abspath(os.path.join(*path_parts))

def file_exists(filepath):
    try:
        if os.path.exists(filepath):
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking file existence {filepath}: {e}")
        return False

# _____________________________________________________________________________
# Reads content from Excel or CSV files and returns a DataFrame
# If the file cannot be read, it returns an empty DataFrame and prints an error message
def file_read(filepath, sheet_name='Ark1', sep=';', header=0):
    df = pd.DataFrame()
    if not file_exists(filepath):
        print(f"File not found: {filepath}")
        return df
    try:
        if filepath.endswith('.xlsx'):
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=header)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath, sep=sep, header=header)
        else:
            raise ValueError("Unsupported file type")
        df.columns = [column.strip().upper().replace(" ", "_").replace("Æ", "AE").replace("Ø", "O").replace("Å", "AA") for column in df.columns]
        return df
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return df

# _____________________________________________________________________________
# Create folder from path if it does not exist, and write alias file in it
def write_folder_alias(alias, path, file_path):
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error creating folder {path}: {e}")
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(str(alias))
    except Exception as e:
        print(f"Error writing alias file {file_path}: {e}")

# _____________________________________________________________________________
# Save a list of lines to a .px file
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
