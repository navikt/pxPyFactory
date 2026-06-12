import pxpyfactory.config
import pxpyfactory.helpers
import os
import shutil
from datetime import datetime, timezone

LOCAL_INPUT = os.path.join(os.path.dirname(__file__), '..', 'input_bucket')
LOCAL_OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'output_bucket')

# _____________________________________________________________________________
def _get_local_path(in_path):
    in_path = str(in_path).strip().lstrip('/')
    if in_path.startswith(('px/', 'sq/', pxpyfactory.config.paths.OUTPUT + '/', pxpyfactory.config.paths.SAVED_QUERY_OUTPUT + '/')):
        return os.path.normpath(os.path.join(LOCAL_OUTPUT, in_path))
    return os.path.normpath(os.path.join(LOCAL_INPUT, in_path))

# _____________________________________________________________________________
def file_exists(file_path):
    return os.path.exists(_get_local_path(file_path))

# _____________________________________________________________________________
def get_file_info(file_path):
    try:
        local_path = _get_local_path(file_path)
        if not os.path.exists(local_path):
            return None, None
        file_size = os.path.getsize(local_path)
        raw_time = datetime.fromtimestamp(os.path.getmtime(local_path), tz=timezone.utc)
        return file_size, raw_time
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error reading file info {file_path}: {e}", 1)
        return None, None

# _____________________________________________________________________________
def _get_folder_info(folder_path, ignore=None):
    local_path = _get_local_path(folder_path)
    if not os.path.isdir(local_path):
        return 0, None
    total_size = 0
    latest_time = None
    for root, dirs, files in os.walk(local_path):
        for fname in files:
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, local_path).replace('\\', '/')
            if ignore is not None and rel == ignore:
                continue
            total_size += os.path.getsize(fpath)
            mtime = datetime.fromtimestamp(os.path.getmtime(fpath), tz=timezone.utc)
            if latest_time is None or mtime > latest_time:
                latest_time = mtime
    return total_size, latest_time

# _____________________________________________________________________________
def _list_files_in_path(folder_path):
    local_path = _get_local_path(folder_path)
    if not os.path.isdir(local_path):
        return []

    files = []
    for root, _, filenames in os.walk(local_path):
        for fname in filenames:
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, local_path).replace('\\', '/')
            files.append(rel)
    return files

# _____________________________________________________________________________
def _read_file(file_path, download_as_bytes=False):
    try:
        local_path = _get_local_path(file_path)
        if not os.path.exists(local_path):
            pxpyfactory.helpers.print_filter(f"File not found: {file_path}", 1)
            return None
        mode = 'rb' if download_as_bytes else 'r'
        encoding = None if download_as_bytes else 'utf-8'
        with open(local_path, mode, encoding=encoding) as f:
            return f.read()
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error reading file {file_path}: {e}", 1)
        return None

# _____________________________________________________________________________
def _write_file(file_path, content):
    try:
        local_path = _get_local_path(file_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(str(content))
        return True
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error writing file {file_path}: {e}", 1)
        return False

# _____________________________________________________________________________
def delete_content_in_path(folder_path):
    local_path = _get_local_path(folder_path)
    if not os.path.isdir(local_path):
        return
    for entry in os.listdir(local_path):
        entry_path = os.path.join(local_path, entry)
        try:
            if os.path.isfile(entry_path):
                os.remove(entry_path)
            elif os.path.isdir(entry_path):
                shutil.rmtree(entry_path)
            pxpyfactory.helpers.print_filter(f"Deleted: {entry_path}", 1)
        except Exception as e:
            pxpyfactory.helpers.print_filter(f"Error deleting {entry_path}: {e}", 1)
