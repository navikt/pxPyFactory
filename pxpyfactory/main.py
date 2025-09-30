import pandas as pd
import os
from pxpyfactory.data_product import PXDataProduct
from pxpyfactory.read_write import write_px
from pxpyfactory.folder_alias import update_folder_structure
from pxpyfactory.utils import prepare_data_products, prepare_metadata_base

class PXMain:
    def __init__(self):
        # Set folders and file-paths
        script_path = os.path.dirname(os.path.abspath(__file__)) # Get path for this script location
        os.chdir(script_path) # Set working directory

        self.input_path = os.path.abspath(os.path.join(script_path, '..', 'input')) # Define input path relative to script location
        self.output_path = os.path.abspath(os.path.join(script_path, '..', 'output'))  # Define output path relative to script location
        self.common_meta_filepath = os.path.abspath(os.path.join(self.input_path, 'common_meta.xlsx')) # Define path to common metadata file

    def run(self):
        data_products_df = prepare_data_products(self.common_meta_filepath) # Get and prepare data products for px file generation from Excel-sheet.
        self.metadata_base = prepare_metadata_base(self.common_meta_filepath) # Get and prepare metadata_base.
        update_folder_structure(data_products_df, self.output_path) # Create folder structure from data_products dataframe

        # Process each data product:
        for _, row in data_products_df.iterrows():
            px_data_product = PXDataProduct(self, row)
            write_px(px_data_product.list_of_lines, px_data_product.px_output_path)

