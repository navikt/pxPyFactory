import pandas as pd
import pxpyfactory.io_utils
import pxpyfactory.utils

# _____________________________________________________________________________
# Log production of current px file to production_log.jsonl
def log_file_production(px_data_product):
    size, time = pxpyfactory.io_utils.get_file_info(px_data_product.table_path)
    meta_size, meta_time = pxpyfactory.io_utils.get_file_info(px_data_product.table_meta_path)
    current_entry_dict = {
        'table_ref': px_data_product.table_ref,
        'timestamp': pxpyfactory.utils.get_time_formatted(),
        'hashed_params': px_data_product.hashed_params,
        'file_size': size if size is not None else '-',
        'mod_time': pxpyfactory.utils.get_time_formatted(time) if time is not None else '-',
        'meta_file_size': meta_size if meta_size is not None else '-',
        'meta_mod_time': pxpyfactory.utils.get_time_formatted(meta_time) if meta_time is not None else '-'
    }
    # px_data_product.main_app.production_log.loc[len(px_data_product.main_app.production_log)] = current_entry_dict
    px_data_product.main_app.production_log = pd.concat([px_data_product.main_app.production_log, pd.DataFrame([current_entry_dict])], ignore_index=True)
    file_path = px_data_product.main_app.production_log_filepath
    content = px_data_product.main_app.production_log.to_json(orient='records', lines=True)
    return pxpyfactory.io_utils.file_write(file_path, content)  # True if file append successful+
    
# _____________________________________________________________________________
# Compare current input to production, with input to latest production of the same table
def object_changed(in_object, prod_log):
    # Is input path a file or folder ?
    if isinstance(in_object, str):
        if '.' in in_object.split('/')[-1]:
            print("File")
        else:
            print("Folder")
    else:
        return input_changed(in_object, prod_log)
# _____________________________________________________________________________
# Compare current input to production, with input to latest production of the same table
def input_changed(px_data_product, prod_log):
    try:
        latest_entry = prod_log[prod_log['table_ref'] == px_data_product.table_ref].sort_values('timestamp').iloc[-1]
    except Exception as e:
        pxpyfactory.utils.print_filter(f"No prior production logged.", 2)
        return True
    output_str = (f"Latest production was\n{latest_entry}\nFirst input update:")
    size, time = pxpyfactory.io_utils.get_file_info(px_data_product.table_path)
    meta_size, meta_time = pxpyfactory.io_utils.get_file_info(px_data_product.table_meta_path)
    if not pxpyfactory.utils.same_value(latest_entry['hashed_params'], px_data_product.hashed_params):
        pxpyfactory.utils.print_filter(f"{output_str} hashed_params: '{latest_entry['hashed_params']}' -> '{px_data_product.hashed_params}'", 2)
        return True
    if not pxpyfactory.utils.same_value(latest_entry['file_size'], size):
        pxpyfactory.utils.print_filter(f"{output_str} file_size: '{latest_entry['file_size']}' -> '{size}'", 2)
        return True
    if not pxpyfactory.utils.same_value(latest_entry['mod_time'], time):
        pxpyfactory.utils.print_filter(f"{output_str} mod_time: '{latest_entry['mod_time']}' -> '{time}'", 2)
        return True
    if not pxpyfactory.utils.same_value(latest_entry['meta_file_size'], meta_size):
        pxpyfactory.utils.print_filter(f"{output_str} meta_file_size: '{latest_entry['meta_file_size']}' -> '{meta_size}'", 2)
        return True
    if not pxpyfactory.utils.same_value(latest_entry['meta_mod_time'], meta_time):
        pxpyfactory.utils.print_filter(f"{output_str} meta_mod_time : '{latest_entry['meta_mod_time']}' -> '{meta_time}'", 2)
        return True

    return False
