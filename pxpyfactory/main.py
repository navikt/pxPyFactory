import pandas as pd
from pxpyfactory.data_product import PXDataProduct
from pxpyfactory.io_utils import get_path, file_read, write_px
from pxpyfactory.utils import prepare_data_products, prepare_metadata_base, prepare_alias, update_folder_structure, print_filter

class PXMain:
    def __init__(self):
        # Set folders and file-paths
        # script_path = set_script_path()
        self.input_path              = 'input' # Define input path relative to script location
        self.output_path             = 'px'  # Define output path relative to script location
        self.common_meta_filepath    = get_path([self.input_path, 'common_meta.xlsx']) # Define path to common metadata file
        self.production_log_filepath = get_path([self.input_path, 'production_log.jsonl']) # Define path to common metadata file

    def run(self):
        self.alias_df       = prepare_alias(self.common_meta_filepath) # Get and prepare alias
        data_products_df    = prepare_data_products(self.common_meta_filepath) # Get and prepare data products for px file generation from Excel-sheet.
        self.metadata_base  = prepare_metadata_base(self.common_meta_filepath) # Get and prepare metadata_base
        self.production_log = file_read(self.production_log_filepath, clean=False) # Read production log file

        update_folder_structure(data_products_df, self.alias_df, self.output_path) # Create folder structure from data_products dataframe
        px_files_written = 0
        sq_file_pairs_written = 0
        # Process each data product:
        for i, row in data_products_df.iterrows():
            print_filter(f"\n--- Start processing data product / table from orderline: {i+2} ---", 1)
            px_data_product = PXDataProduct(self, row)
            if px_data_product.make_px():
                # If px content was created successfully, continue to write px file
                if write_px(px_data_product.list_of_lines, px_data_product.px_output_path):
                    # If writing px file to disk is successful, log the production
                    if px_data_product.log_file_production():
                        # If logging is successful, print confirmation
                        print_filter(f"PX file successfully written: {px_data_product.px_output_path}", 1)
                        px_files_written += 1
                    if px_data_product.make_sq(): # Create a standard Saved Query for the px file
                        print("Saved Query files generated")
                        sq_file_pairs_written += 1

        print_filter(f"\n--- PX file generation completed. Total PX files written: {px_files_written} (included saved query file pairs: {sq_file_pairs_written}) ---", 0)
