import os
import shutil
import pandas as pd
import io
import pxpyfactory.config
import pxpyfactory.helpers
import pxpyfactory.validation

LOCAL_INPUT = os.path.join(os.path.dirname(__file__), '..', 'input_bucket')
LOCAL_OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'output_bucket')

# _____________________________________________________________________________
def _is_path_to_file(in_path):
    if in_path is None:
        return None
    in_path = str(in_path).strip()
    if '.' in in_path:
        return True
    if '/' in in_path:
        return False
    return None

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
def get_last_updated(in_path):
    file_size, raw_time = get_file_info(in_path)
    if raw_time is None:
        return ''
    return pxpyfactory.validation.get_time_formatted(raw_time)

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

# _____________________________________________________________________________
def get_path_info(in_path, ignore=None):
    if '.' in in_path:
        return get_file_info(in_path)
    else:
        return _get_folder_info(in_path, ignore=ignore)

# _____________________________________________________________________________
def get_file_info(file_path):
    try:
        local_path = _get_local_path(file_path)
        if not os.path.exists(local_path):
            return None, None
        from datetime import datetime, timezone
        file_size = os.path.getsize(local_path)
        raw_time = datetime.fromtimestamp(os.path.getmtime(local_path), tz=timezone.utc)
        return file_size, raw_time
    except Exception as e:
        pxpyfactory.helpers.print_filter(f"Error reading file info {file_path}: {e}", 1)
        return None, None

# _____________________________________________________________________________
def _get_folder_info(folder_path, ignore=None):
    from datetime import datetime, timezone
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
def _read_local_file(file_path, download_as_bytes=False):
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
def _write_local_file(file_path, content):
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
def file_read(file_path, sheet_name='Ark1', sep=';', header=0, clean=True):
    df = pd.DataFrame()
    if not file_exists(file_path):
        pxpyfactory.helpers.print_filter(f"File not found: {file_path}", 1)
        return df
    try:
        if file_path.endswith('.xlsx'):
            content = _read_local_file(file_path, download_as_bytes=True)
            if content is None:
                return df
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=header)
        elif file_path.endswith('.csv'):
            content = _read_local_file(file_path)
            if content is None:
                return df
            content = '\n'.join([line for line in content.split('\n') if not line.strip().startswith('--')])
            df = pd.read_csv(io.StringIO(content), sep=sep, decimal=',', header=header, **({'engine': 'python'} if sep is None else {}))
        elif file_path.endswith('.jsonl'):
            content = _read_local_file(file_path)
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
def file_write(file_path, content):
    if pxpyfactory.helpers.get_input_args('test') or pxpyfactory.helpers.get_input_args('test_full'):
        content_lines = content.split('\n')
        if len(content_lines) > 1:
            pxpyfactory.helpers.print_filter('/// file_write - ' + file_path + ' \\\\', 0)
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
        return _write_local_file(file_path, content)
