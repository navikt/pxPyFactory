import io
import pandas as pd
import pxpyfactory.helpers
import pxpyfactory.validation

# Choose backend based on config
try:
    from pxpyfactory import file_io_gcs as _backend
except ImportError:
    from pxpyfactory import file_io_local as _backend

# Backend-specific low-level functions
_backend_read_file = _backend._read_file
_backend_write_file = _backend._write_file
_backend_file_exists = _backend.file_exists
_backend_get_file_info = _backend.get_file_info
_backend_get_folder_info = _backend._get_folder_info
_backend_list_files_in_path = _backend._list_files_in_path
delete_content_in_path = _backend.delete_content_in_path

_path_lookup_cache = {}


def _normalize_path(path):
    return str(path).strip().replace('\\', '/').lstrip('/')


def _split_parent_and_name(path):
    if '/' in path:
        parent, name = path.rsplit('/', 1)
        return parent, name
    return '', path


def _resolve_file_path(path):
    normalized = _normalize_path(path)
    if normalized == '':
        return normalized

    if _backend_file_exists(normalized):
        return normalized

    parent, name = _split_parent_and_name(normalized)
    cache_key = parent.lower()
    files_in_parent = _path_lookup_cache.get(cache_key)
    if files_in_parent is None:
        files_in_parent = _backend_list_files_in_path(parent)
        _path_lookup_cache[cache_key] = files_in_parent

    name_lower = name.lower()
    for rel_path in files_in_parent:
        rel_norm = str(rel_path).strip().lstrip('/')
        if rel_norm.lower() == name_lower:
            return '/'.join([parent, rel_norm]).strip('/') if parent else rel_norm

    all_files_cache_key = '__all_files__'
    all_files = _path_lookup_cache.get(all_files_cache_key)
    if all_files is None:
        all_files = _backend_list_files_in_path('')
        _path_lookup_cache[all_files_cache_key] = all_files

    normalized_lower = normalized.lower()
    for rel_path in all_files:
        rel_norm = str(rel_path).strip().lstrip('/')
        if rel_norm.lower() == normalized_lower:
            return rel_norm

    return normalized


def file_exists(file_path):
    resolved_path = _resolve_file_path(file_path)
    return _backend_file_exists(resolved_path)


def get_file_info(file_path):
    resolved_path = _resolve_file_path(file_path)
    return _backend_get_file_info(resolved_path)

# _____________________________________________________________________________
def get_last_updated(in_path):
    file_size, raw_time = get_file_info(in_path)
    if raw_time is None:
        return ''
    return pxpyfactory.validation.get_time_formatted(raw_time)

# _____________________________________________________________________________
def get_path_info(in_path, ignore=None):
    if '.' in in_path:
        return get_file_info(in_path)
    return _backend_get_folder_info(in_path, ignore=ignore)

# _____________________________________________________________________________
def list_files_in_path(in_path):
    return _backend_list_files_in_path(in_path)

# _____________________________________________________________________________
def file_read(file_path, sheet_name='Ark1', sep=';', header=0, clean=True):
    df = pd.DataFrame()
    resolved_path = _resolve_file_path(file_path)
    if not _backend_file_exists(resolved_path):
        pxpyfactory.helpers.print_filter(f"File not found: {file_path}", 1)
        return df
    try:
        file_ext = resolved_path.lower()
        if file_ext.endswith('.xlsx'):
            content = _backend_read_file(resolved_path, download_as_bytes=True)
            if content is None:
                return df
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=header)
        elif file_ext.endswith('.csv'):
            content = _backend_read_file(resolved_path)
            if content is None:
                return df
            content = '\n'.join([line for line in content.split('\n') if not line.strip().startswith('--')])
            csv_kwargs = {
                'sep': sep,
                'decimal': ',',
                'header': header,
                'quotechar': '"',
                'doublequote': True,
                'skipinitialspace': True,
            }

            # sep=None means pandas should auto-detect delimiter, which requires python engine.
            if sep is None:
                csv_kwargs['engine'] = 'python'

            df = pd.read_csv(io.StringIO(content), **csv_kwargs)
        elif file_ext.endswith('.jsonl'):
            content = _backend_read_file(resolved_path)
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
    _path_lookup_cache.clear()
    return _backend_write_file(file_path, content)
