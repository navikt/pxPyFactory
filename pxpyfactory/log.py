import pandas as pd
from pxpyfactory import data_product
import pxpyfactory.io_utils
import pxpyfactory.utils
import json
from datetime import datetime
    
class PXLog:
    def __init__(self, main_app, production_log_filepath):
        self.main_app = main_app
        self.filepath = production_log_filepath
        self.log_history = pxpyfactory.io_utils.file_read(production_log_filepath, clean=False) # Read production log file
        self.log_current = []
        self.alias_built = False

# _____________________________________________________________________________
    def log_data_product(self, data_product):
        pxpyfactory.utils.print_filter(f"log_data_product({data_product})", 3)
        current_dict = self._get_current_data_product_entry(data_product)
        self.log_current.append(current_dict)
# _____________________________________________________________________________
    def write_log(self, px_files_written_ref):
        current_entry_dict = self._get_current_summary_entry(px_files_written_ref)
        self.log_current.append(current_entry_dict)

        # Combine history and current entries, filter out invalid values, and convert to JSON lines
        all_records = list(self.log_history.to_dict('records')) + self.log_current
        # When content is written, only include valid values. This will hide all logged stuff that is regarded as empty or not valid.
        json_lines = []
        for record in all_records:
            clean_record = {}
            for key, value in record.items():
                if pxpyfactory.utils.valid_value(value, full=True):
                    if isinstance(value, datetime):
                        clean_record[key] = pxpyfactory.utils.get_time_formatted(value)
                    else:
                        clean_record[key] = value
            json_lines.append(json.dumps(clean_record))
        content = '\n'.join(json_lines) + '\n'
        
        write_success = pxpyfactory.io_utils.file_write(self.filepath, content)
        return write_success
# _____________________________________________________________________________
    def _get_current_summary_entry(self, px_files_written_ref=None):
        input_size, input_time = pxpyfactory.io_utils.get_path_info(self.main_app.input_path, ignore = self.filepath)
        common_meta_size, common_meta_time = pxpyfactory.io_utils.get_path_info(self.main_app.common_meta_filepath)
        current_dict = {}
        current_dict = {
            'timestamp': pxpyfactory.utils.get_time_formatted(),
            'type': 'summary',
            'input_size': input_size,
            'input_time': input_time,
            "common_meta_size": common_meta_size,
            "common_meta_time": common_meta_time,
            'alias_built': self.alias_built,
            'tables_built': px_files_written_ref
        }
        return current_dict
# _____________________________________________________________________________
    def _get_current_data_product_entry(self, data_product):
        size, time = pxpyfactory.io_utils.get_file_info(data_product.table_path)
        meta_size, meta_time = pxpyfactory.io_utils.get_file_info(data_product.table_meta_path)
        current_dict = {}
        current_dict = {
            'timestamp': pxpyfactory.utils.get_time_formatted(),
            'type': 'table',
            'table_ref': data_product.table_ref,
            'hashed_params': data_product.hashed_params,
            'size': size if size is not None else '-',
            'time': pxpyfactory.utils.get_time_formatted(time) if time is not None else '-',
            'meta_size': meta_size if meta_size is not None else '-',
            'meta_time': pxpyfactory.utils.get_time_formatted(meta_time) if meta_time is not None else '-'
        }

        return current_dict
# _____________________________________________________________________________
    def _get_latest_entry(self, type, table_ref=None):
            try:
                log_history_filtered = self.log_history[self.log_history['type'] == type]
                if table_ref is not None:
                    log_history_filtered = log_history_filtered[log_history_filtered['table_ref'] == table_ref]
                return log_history_filtered.sort_values('timestamp').iloc[-1]
            except Exception as e:
                pxpyfactory.utils.print_filter(f"No prior production logged.", 2)
                return None
# _____________________________________________________________________________
# Compare current object to latest logged item of the same object
    def input_change(self):
        pxpyfactory.utils.print_filter(f"input_change", 3)
        latest_entry = self._get_latest_entry('summary')
        current_entry = self._get_current_summary_entry()
        keys_to_check = ['input_size', 'input_time']
        return self._check_diff(latest_entry, current_entry, keys_to_check)
# _____________________________________________________________________________
# Compare current object to latest logged item of the same object
    def common_meta_change(self):
        pxpyfactory.utils.print_filter(f"common_meta_change", 3)
        latest_entry = self._get_latest_entry('summary')
        current_entry = self._get_current_summary_entry()
        keys_to_check = ['common_meta_size', 'common_meta_time']
        return self._check_diff(latest_entry, current_entry, keys_to_check)
    # _____________________________________________________________________________
# Compare current object to latest logged item of the same object
    def data_product_change(self, data_product):
        pxpyfactory.utils.print_filter(f"data_product_change({data_product})", 3)
        latest_entry = self._get_latest_entry('table', data_product.table_ref)
        current_entry = self._get_current_data_product_entry(data_product)
        keys_to_check = ['hashed_params', 'size', 'time', 'meta_size', 'meta_time']
        return self._check_diff(latest_entry, current_entry, keys_to_check)
# _____________________________________________________________________________
# Compare current object to latest logged item of the same object
    def _check_diff(self, entry1, entry2, keys_to_check):
        if entry1 is None or entry2 is None:
            return True  # No previous entry
        for key in keys_to_check:
            if not pxpyfactory.utils.same_value(entry1[key], entry2[key]):
                pxpyfactory.utils.print_filter(f"Input change detected: {key}: '{entry1[key]}' -> '{entry2[key]}'", 2)
                return True
        return False
    
