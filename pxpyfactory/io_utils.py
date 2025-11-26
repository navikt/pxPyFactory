from google.cloud import storage # Imports the Google Cloud client library
import pandas as pd
import io
# import pprint
# import os
# import json
from datetime import datetime

storage_client = storage.Client() # Instantiates a client
bucket_name = "pxweb2-api-nais-test" # The name for the new bucket
bucket = storage_client.bucket(bucket_name)

# _____________________________________________________________________________
def get_path(path_parts):
    return "/".join(path_parts)

def file_exists(file_path):
    try:
        return storage.Blob(bucket=bucket, name=file_path).exists(storage_client)
    except Exception as e:
        print(f"Error checking file existence {file_path}: {e}")
        return False
# _____________________________________________________________________________
# Reads a file from Google Cloud Storage and returns its content as a string.
def read_gcs_file(source_blob_name, download_as_bytes=False):
    try:
        blob = bucket.blob(source_blob_name)
        if download_as_bytes:
          content = blob.download_as_bytes()
        else:
          content = blob.download_as_text()
        return content
    except Exception as e:
        print(f"Error reading file {source_blob_name} from bucket {bucket_name}: {e}")
        return None

# _____________________________________________________________________________
def write_gcs_file(destination_blob_name, content):
    try:
        blob = bucket.blob(str(destination_blob_name))
        blob.upload_from_string(str(content))
        return True
    except Exception as e:
        print(f"Error writing file {destination_blob_name} to bucket {bucket_name}: {e}")
        return None
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
            content = read_gcs_file(file_path, download_as_bytes=True)
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=header)
        elif file_path.endswith('.csv'):
            content = read_gcs_file(file_path)
            df = pd.read_csv(io.StringIO(content), sep=sep, header=header)
        elif file_path.endswith('.jsonl'):
            content = read_gcs_file(file_path)
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
def write_log(file_path, content_df):
    return write_gcs_file(file_path, content_df.to_json(orient='records', lines=True))
# _____________________________________________________________________________
# Create folder from path if it does not exist, and write alias file in it
def write_folder_alias(alias, path, file_path):

    write_gcs_file(file_path, alias)
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
# _____________________________________________________________________________