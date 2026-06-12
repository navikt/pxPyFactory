import pxpyfactory.config
import pxpyfactory.helpers
from google.cloud import storage

storage_client = storage.Client()
bucket_input = storage_client.bucket(pxpyfactory.config.gcs.BUCKET_INPUT)
bucket_output = storage_client.bucket(pxpyfactory.config.gcs.BUCKET_OUTPUT)

# _____________________________________________________________________________
def _get_full_path(in_path):
    in_path = str(in_path).strip().lstrip('/')
    if '.' not in in_path and not in_path.endswith('/'):
        in_path += '/'
    return in_path

# _____________________________________________________________________________
def _get_bucket(in_path):
    in_path = str(in_path).strip().lstrip('/')
    if in_path.startswith(('px/', 'sq/', pxpyfactory.config.paths.OUTPUT + '/', pxpyfactory.config.paths.SAVED_QUERY_OUTPUT + '/')):
        return bucket_output
    return bucket_input

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
def get_file_info(file_path):
    try:
        file_blob = _get_file_blob(file_path, with_metadata=True)
        if file_blob is None:
            return None, None
        file_size = file_blob.size
        raw_time = file_blob.updated
        return file_size, raw_time
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error reading file info {file_path}: {e}", 1)
        return None, None

# _____________________________________________________________________________
def _get_folder_info(folder_path, ignore=None):
    if not folder_path.endswith('/'):
        folder_path += '/'
    bucket = _get_bucket(folder_path)
    blobs = bucket.list_blobs(prefix=_get_full_path(folder_path))
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
def _list_files_in_path(folder_path):
    if not folder_path.endswith('/'):
        folder_path += '/'

    prefix = _get_full_path(folder_path)
    bucket = _get_bucket(folder_path)
    blobs = bucket.list_blobs(prefix=prefix)

    files = []
    for blob in blobs:
        if blob.name.endswith('/'):
            continue
        rel = blob.name[len(prefix):] if blob.name.startswith(prefix) else blob.name
        if rel != '':
            files.append(rel)
    return files

# _____________________________________________________________________________
def _read_file(file_path, download_as_bytes=False):
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
def _write_file(file_path, content):
    try:
        file_blob = _get_file_blob(file_path, with_metadata=False)
        file_blob.upload_from_string(str(content))
        return True
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error writing file {file_path}: {e}", 1)
        return False

# _____________________________________________________________________________
def delete_content_in_path(folder_path):
    if not folder_path.endswith('/'):
        folder_path += '/'
    bucket = _get_bucket(folder_path)
    blobs = bucket.list_blobs(prefix=_get_full_path(folder_path))
    for blob in blobs:
        try:
            blob.delete()
            pxpyfactory.helpers.print_filter(f"Deleted: {blob.name}", 1)
        except Exception as e:
            pxpyfactory.helpers.print_filter(f"Error deleting file {blob.name}: {e}", 1)
