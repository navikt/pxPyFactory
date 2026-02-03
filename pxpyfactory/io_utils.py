from google.cloud import storage # Imports the Google Cloud client library
import pandas as pd
import io
import pxpyfactory.utils

storage_client = storage.Client() # Instantiates a client
# bucket_name = "pxweb2-api-nais-test" # The name for the new bucket
bucket_name = "pxweb2-api-nais-px" # The name for the new bucket
bucket = storage_client.bucket(bucket_name)

# _____________________________________________________________________________
def get_path(path_parts):
    return "/".join(path_parts)
# _____________________________________________________________________________
def file_exists(file_path):
    try:
        return storage.Blob(bucket=bucket, name=file_path).exists(storage_client)
    except Exception as e:
        pxpyfactory.utils.print_filter(f"Error checking file existence {file_path}: {e}", 1)
        return False
# _____________________________________________________________________________
def get_last_updated(in_path):
    file_size, raw_time = get_file_info(in_path)
    if raw_time is None:
        return ''
    return pxpyfactory.utils.get_time_formatted(raw_time)

# _____________________________________________________________________________
def get_path_info(in_path, ignore=None):
    # Determine if path is a file or folder
    if '.' in in_path:
        return get_file_info(in_path)
    else:
        return get_folder_info(in_path, ignore=ignore)
    # if '.' in path.split('/')[-1]:

# _____________________________________________________________________________
def get_file_info(file_path):
    if file_exists(file_path):
        file_blob = bucket.get_blob(file_path)
        file_size = file_blob.size # Get file size in bytes
        raw_time = file_blob.updated # Get last modified time (as a timestamp)
        # mod_time = datetime.fromtimestamp(raw_time) #.strftime("%Y%m%d %H:%M:%S")
        # pxpyfactory.utils.print_filter(f"File info for {file_path}: size={file_size}, mod_time={mod_time}", 1)
        return file_size, raw_time #mod_time
    else:
        return None, None
# _____________________________________________________________________________
def get_folder_info(folder_path, ignore=None):
    if not folder_path.endswith('/'):
        folder_path += '/'
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    total_size = 0
    latest_time = None
    for blob in blobs:
        if ignore is not None and blob.name == ignore:
            continue
        total_size += blob.size
        if latest_time is None or blob.updated > latest_time:
            latest_time = blob.updated
    return total_size, latest_time
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
        pxpyfactory.utils.print_filter(f"Error reading file {source_blob_name} from bucket {bucket_name}: {e}", 1)
        return None
# _____________________________________________________________________________
# Write content to a file in Google Cloud Storage. If file dont exist, it is created.
def write_gcs_file(destination_blob_name, content):
    try:
        blob = bucket.blob(str(destination_blob_name))
        blob.upload_from_string(str(content))
        return True
    except Exception as e:
        pxpyfactory.utils.print_filter(f"Error writing file {destination_blob_name} to bucket {bucket_name}: {e}", 1)
        return False
# _____________________________________________________________________________
# Reads content from Excel or CSV files and returns a DataFrame
# If the file cannot be read, it returns an empty DataFrame and prints an error message
def file_read(file_path, sheet_name='Ark1', sep=';', header=0, clean=True):
    df = pd.DataFrame()
    if not file_exists(file_path):
        pxpyfactory.utils.print_filter(f"File not found: {file_path}", 1)
        return df
    try:
        if file_path.endswith('.xlsx'):
            content = read_gcs_file(file_path, download_as_bytes=True)
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=header)
        elif file_path.endswith('.csv'):
            content = read_gcs_file(file_path)
             # Use python engine if sep is None to auto-detect separator, else default engine (engine is not spesified)
            df = pd.read_csv(io.StringIO(content), sep=sep, decimal=',', header=header, **({'engine': 'python'} if sep is None else {}))
        elif file_path.endswith('.jsonl'):
            content = read_gcs_file(file_path)
            df = pd.read_json(io.StringIO(content), lines=True, convert_dates=False)
        else:
            raise ValueError("Unsupported file type")
        if clean:
            df.columns = [column.strip().upper().replace(" ", "_").replace("Æ", "AE").replace("Ø", "O").replace("Å", "AA") for column in df.columns]
        return df
    except Exception as e:
        pxpyfactory.utils.print_filter(f"Error reading file {file_path}: {e}", 1)
        return df
# _____________________________________________________________________________
# Create folder from path if it does not exist, and write file in it
def file_write(file_path, content):
    if pxpyfactory.utils.get_input_args('test'): # Print to console if no file path is given
        pxpyfactory.utils.print_filter('/// file_write - ' + file_path + ' \\\\\\', 0)
        for line in content.split('\n'):
            pxpyfactory.utils.print_filter(line[:200], 0)
        pxpyfactory.utils.print_filter('\\\\\\ file_write - ' + file_path + ' ///', 0)
        return False
    else:
        return write_gcs_file(file_path, content)
