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
_read_file = _backend._read_file
_write_file = _backend._write_file
file_exists = _backend.file_exists
get_file_info = _backend.get_file_info
_get_folder_info = _backend._get_folder_info
delete_content_in_path = _backend.delete_content_in_path

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
    return _get_folder_info(in_path, ignore=ignore)

# _____________________________________________________________________________
def file_read(file_path, sheet_name='Ark1', sep=';', header=0, clean=True):
    df = pd.DataFrame()
    if not file_exists(file_path):
        pxpyfactory.helpers.print_filter(f"File not found: {file_path}", 1)
        return df
    try:
        if file_path.endswith('.xlsx'):
            content = _read_file(file_path, download_as_bytes=True)
            if content is None:
                return df
            df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=header)
        elif file_path.endswith('.csv'):
            content = _read_file(file_path)
            if content is None:
                return df
            content = '\n'.join([line for line in content.split('\n') if not line.strip().startswith('--')])
            df = pd.read_csv(io.StringIO(content), sep=sep, decimal=',', header=header, **({'engine': 'python'} if sep is None else {}))
        elif file_path.endswith('.jsonl'):
            content = _read_file(file_path)
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
    return _write_file(file_path, content)
