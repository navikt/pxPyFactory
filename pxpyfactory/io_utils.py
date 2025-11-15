import pandas as pd
# import pprint
import os
import json
from datetime import datetime

def set_script_path():
    # Set folders and file-paths
    script_path = os.path.dirname(os.path.abspath(__file__)) # Get path for this script location
    os.chdir(script_path) # Set working directory
    return script_path

def get_path(*path_parts):
    return os.path.abspath(os.path.join(*path_parts))

def file_exists(file_path):
    try:
        if os.path.exists(file_path):
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking file existence {file_path}: {e}")
        return False

# _____________________________________________________________________________
# Reads content from Excel or CSV files and returns a DataFrame
# If the file cannot be read, it returns an empty DataFrame and prints an error message
def file_read(file_path, sheet_name='Ark1', sep=';', header=0, clean=True):
    df = pd.DataFrame()
    if not file_exists(file_path):
        print(f"File not found: {file_path}")
        return df
    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path, sep=sep, header=header)
        elif file_path.endswith('.jsonl'):
            df = pd.read_json(file_path, lines=True)
        else:
            raise ValueError("Unsupported file type")
        if clean:
            df.columns = [column.strip().upper().replace(" ", "_").replace("Æ", "AE").replace("Ø", "O").replace("Å", "AA") for column in df.columns]
        return df
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return df

# _____________________________________________________________________________
# Reads content from Excel or CSV files and returns a DataFrame
# If the file cannot be read, it returns an empty DataFrame and prints an error message
def file_append(file_path, entry_dict):
    success = False
    if file_exists(file_path):
        try:
            if file_path.endswith('.jsonl'):
                with open(file_path, "a") as f:
                    f.write(json.dumps(entry_dict) + "\n")
                success = True
            else:
                raise ValueError("Unsupported file type")
        except Exception as e:
            print(f"Error appending to file {file_path}: {e}")
    else:
        print(f"File not found: {file_path}")
    return success

# _____________________________________________________________________________
# 
def get_file_info(file_path):
    if file_exists(file_path):
        file_size = os.path.getsize(file_path) # Get file size in bytes
        raw_time = os.path.getmtime(file_path) # Get last modified time (as a timestamp)
        mod_time = datetime.fromtimestamp(raw_time) #.strftime("%Y%m%d %H:%M:%S")
        # print(f"File info for {file_path}: size={file_size}, mod_time={mod_time}")
        return file_size, mod_time
    else:
        return None, None
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
# Return True if successful writing to file, False otherwise
def write_px(list_of_lines, file_path):
    if file_path is None: # Print to console if no file path is given
        print('#'*80)
        for line in list_of_lines:
            print(line[:200])
        print('#'*80)
        return False
    else:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                for line in list_of_lines:
                    f.write(line + "\n")
            return True
        except Exception as e:
            print(f"Error writing PX file {file_path}: {e}")
            return False
