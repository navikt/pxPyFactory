import pandas as pd
# import pprint
# import os
import io
import json
from datetime import datetime
#########################################################################
# Imports the Google Cloud client library
from google.cloud import storage

# Instantiates a client
storage_client = storage.Client()
# The name for the new bucket
bucket_name = "pxweb2-api-nais-test"
bucket = storage_client.bucket(bucket_name)

def read_gcs_file(source_blob_name):
    """Reads a file from Google Cloud Storage and returns its content as a string."""
    try:
        blob = bucket.blob(source_blob_name)
        content = blob.download_as_text()
        return content
    except Exception as e:
        print(f"Error reading file {source_blob_name} from bucket {bucket_name}: {e}")
        return None

def write_gcs_file(destination_blob_name, content):
    """Reads a file from Google Cloud Storage and returns its content as a string."""
    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        return True
    except Exception as e:
        print(f"Error writing file {destination_blob_name} to bucket {bucket_name}: {e}")
        return None

#########################################################################

def get_path(path_parts):
    # return os.path.abspath(os.path.join(*path_parts))
    return "/".join(path_parts)

def file_exists(file_path):
    try:
        return storage.Blob(bucket=bucket, name=file_path).exists(storage_client)
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
    content = read_gcs_file(file_path)
    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(io.StringIO(content), sheet_name=sheet_name, header=header)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(io.StringIO(content), sep=sep, header=header)
        elif file_path.endswith('.jsonl'):
            df = pd.read_json(io.StringIO(content), lines=True)
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
def write_log(file_path, content_df):
        # if file_path.endswith('.jsonl'):
        #     with open(file_path, "a") as f:
        #         f.write(json.dumps(entry_dict) + "\n")
        #     success = True
    return write_gcs_file(file_path, content_df.to_json(orient='records', lines=True))

# _____________________________________________________________________________
# 
def get_file_info(file_path):
    if file_exists(file_path):
        file_blob = bucket.get_blob(file_path)
        file_size = file_blob.size # Get file size in bytes
        raw_time = file_blob.updated # Get last modified time (as a timestamp)
        # mod_time = datetime.fromtimestamp(raw_time) #.strftime("%Y%m%d %H:%M:%S")
        # print(f"File info for {file_path}: size={file_size}, mod_time={mod_time}")
        return file_size, raw_time #mod_time
    else:
        return None, None
# _____________________________________________________________________________
# Create folder from path if it does not exist, and write alias file in it
def write_folder_alias(alias, path, file_path):

    write_gcs_file(file_path, alias)

    # try:
    #     path.mkdir(parents=True, exist_ok=True)
    # except Exception as e:
    #     print(f"Error creating folder {path}: {e}")
    # try:
    #     with open(file_path, 'w', encoding='utf-8') as f:
    #         f.write(str(alias))
    # except Exception as e:
    #     print(f"Error writing alias file {file_path}: {e}")

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
        write_gcs_file(file_path, "\n".join(list_of_lines))
        return True
    
        # try:
        #     with open(file_path, "w", encoding="utf-8") as f:
        #         for line in list_of_lines:
        #             f.write(line + "\n")
        #     return True
        # except Exception as e:
        #     print(f"Error writing PX file {file_path}: {e}")
        #     return False
