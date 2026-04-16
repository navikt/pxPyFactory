from google.cloud import storage # Imports the Google Cloud client library
import pandas as pd
import io
import pxpyfactory.config
import pxpyfactory.helpers
import pxpyfactory.validation

storage_client = storage.Client() # Instantiates a client

# Setup both buckets
bucket_input = storage_client.bucket(pxpyfactory.config.gcs.BUCKET_INPUT)
bucket_output = storage_client.bucket(pxpyfactory.config.gcs.BUCKET_OUTPUT)


# _____________________________________________________________________________
def _is_path_to_file(in_path):
    # Determine if path is a file or folder
    if in_path is None:
        return None
    in_path = str(in_path).strip()
    if '.' in in_path:
        return True
    if '/' in in_path:
        return False
    else:
        return None

# _____________________________________________________________________________
def _get_full_path(in_path):
        in_path = str(in_path).strip().lstrip('/')
        path_is_to_file = _is_path_to_file(in_path)
        if (not path_is_to_file) and (not in_path.endswith('/')):
            in_path += '/'
        return in_path

# _____________________________________________________________________________
def _get_bucket(in_path):
    in_path = str(in_path).strip().lstrip('/')
    # Output bucket: PX files and saved queries
    if in_path.startswith(('px/', 'sq/', pxpyfactory.config.paths.OUTPUT + '/', pxpyfactory.config.paths.SAVED_QUERY_OUTPUT + '/')):
        return bucket_output
    # Input bucket: everything else (CSV, Excel, logs, work files)
    return bucket_input

# _____________________________________________________________________________
def _get_blob_or_blobs(in_path):
    """Returns appropriate bucket based on file path"""
    in_path = _get_full_path(in_path)
    bucket = _get_bucket(in_path)
    
    if _is_path_to_file(in_path):
        return bucket.blob(in_path)
    else:
        return bucket.list_blobs(prefix=in_path)

# _____________________________________________________________________________
def _get_file_blob(file_path, with_metadata=False):
    file_path = _get_full_path(file_path)
    bucket = _get_bucket(file_path)
    if with_metadata:
        return bucket.get_blob(file_path)
    return bucket.blob(file_path)

# _____________________________________________________________________________
def file_exists(file_path):
    try:
        file_blob = _get_file_blob(file_path, with_metadata=True)
        return file_blob is not None
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error checking file existence {file_path}: {e}", 1)
        return False
# _____________________________________________________________________________
def get_last_updated(in_path):
    file_size, raw_time = get_file_info(in_path)
    if raw_time is None:
        return '' 
    return pxpyfactory.validation.get_time_formatted(raw_time)

# _____________________________________________________________________________
# Clean output folder before creating new structure
def delete_content_in_path(folder_path):
    if not folder_path.endswith('/'):
        folder_path += '/'
    blobs = _get_blob_or_blobs(folder_path)
    for blob in blobs:
        try:
            blob.delete()
            pxpyfactory.helpers.print_filter(f"Deleted: {blob.name}", 1)
        except Exception as e:
            pxpyfactory.helpers.print_filter(f"Error deleting file {blob.name}: {e}", 1)
# _____________________________________________________________________________
def get_path_info(in_path, ignore=None):
    # Determine if path is a file or folder
    if '.' in in_path:
        return get_file_info(in_path)
    else:
        return _get_folder_info(in_path, ignore=ignore)
    # if '.' in path.split('/')[-1]:

# _____________________________________________________________________________
def get_file_info(file_path):
    try:
        file_blob = _get_file_blob(file_path, with_metadata=True)
        if file_blob is None:
            return None, None
        file_size = file_blob.size # Get file size in bytes
        raw_time = file_blob.updated # Get last modified time (as a timestamp)
        return file_size, raw_time
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error reading file info {file_path}: {e}", 1)
        return None, None
# _____________________________________________________________________________
def _get_folder_info(folder_path, ignore=None):
    if not folder_path.endswith('/'):
        folder_path += '/'
    blobs = _get_blob_or_blobs(folder_path)
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
def _read_gcs_file(file_path, download_as_bytes=False):
    try:
        file_blob = _get_file_blob(file_path, with_metadata=True)
        if file_blob is None:
            pxpyfactory.helpers.print_filter(f"File not found: {file_path}", 1)
            return None
        if download_as_bytes:
            content = file_blob.download_as_bytes()
        else:
            content = file_blob.download_as_text()
        return content
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error reading file {file_path}: {e}", 1)
        return None
# _____________________________________________________________________________
# Write content to a file in Google Cloud Storage. If file dont exist, it is created.
def _write_gcs_file(file_path, content):
    try:
        file_blob = _get_file_blob(file_path, with_metadata=False)
        file_blob.upload_from_string(str(content))
        return True
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error writing file {file_path}: {e}", 1)
        return False
# _____________________________________________________________________________
# Reads content from Excel or CSV files and returns a DataFrame
# If the file cannot be read, it returns an empty DataFrame and prints an error message
def file_read(file_path, sheet_name='Ark1', sep=';', header=0, clean=True):
    df = pd.DataFrame()
    if not file_exists(file_path):
        pxpyfactory.helpers.print_filter(f"File not found: {file_path}", 1)
        return df
    try:
        if file_path.endswith('.xlsx'):
            content = _read_gcs_file(file_path, download_as_bytes=True)
            if content is None:
                return df
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=header)
        elif file_path.endswith('.csv'):
            content = _read_gcs_file(file_path)
            if content is None:
                return df
            # Remove lines starting with -- (SQL-style comments)
            content = '\n'.join([line for line in content.split('\n') if not line.strip().startswith('--')])
             # Use python engine if sep is None to auto-detect separator, else default engine (engine is not spesified)
            df = pd.read_csv(io.StringIO(content), sep=sep, decimal=',', header=header, **({'engine': 'python'} if sep is None else {}))
        elif file_path.endswith('.jsonl'):
            content = _read_gcs_file(file_path)
            if content is None:
                return df
            df = pd.read_json(io.StringIO(content), lines=True, convert_dates=False)
        else:
            raise ValueError("Unsupported file type")
        if clean:
            df.columns = [column.strip().upper().replace(" ", "_").replace("Æ", "AE").replace("Ø", "O").replace("Å", "AA") for column in df.columns]
        return df
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error reading file {file_path}: {e}", 1)
        return df
# _____________________________________________________________________________
# Create folder from path if it does not exist, and write file in it
def file_write(file_path, content):
    if pxpyfactory.helpers.get_input_args('test') or pxpyfactory.helpers.get_input_args('test_full'): # Print to console if no file path is given
        content_lines = content.split('\n')
        if len(content_lines) > 1:
            pxpyfactory.helpers.print_filter('/// file_write - ' + file_path + ' \\\\', 0)
            # Print file_path and content on the same line
        else:
            content_lines[0] = f"{file_path}: {content_lines[0]}"
        for line in content_lines:
            if pxpyfactory.helpers.get_input_args('test_full'):
                pxpyfactory.helpers.print_filter(line, 0)
            else:
                pxpyfactory.helpers.print_filter(line[:200], 0)
        if len(content_lines) > 1:
            pxpyfactory.helpers.print_filter('\\\\\ file_write - ' + file_path + ' ///', 0)
        return False
    else:
        return _write_gcs_file(file_path, content)
