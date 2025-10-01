import pandas as pd
from pxpyfactory.data_product import PXDataProduct
from pxpyfactory.folder_alias import update_folder_structure
from pxpyfactory.io_utils import set_script_path, get_path, write_px
from pxpyfactory.utils import prepare_data_products, prepare_metadata_base

class PXMain:
    def __init__(self):
        # Set folders and file-paths
        script_path = set_script_path()
        self.input_path             = get_path(script_path, '..', 'input') # Define input path relative to script location
        self.output_path            = get_path(script_path, '..', 'output')  # Define output path relative to script location
        self.common_meta_filepath   = get_path(self.input_path, 'common_meta.xlsx') # Define path to common metadata file

    def run(self):
        data_products_df   = prepare_data_products(self.common_meta_filepath) # Get and prepare data products for px file generation from Excel-sheet.
        self.metadata_base = prepare_metadata_base(self.common_meta_filepath) # Get and prepare metadata_base.
        update_folder_structure(data_products_df, self.output_path) # Create folder structure from data_products dataframe

        # Process each data product:
        for _, row in data_products_df.iterrows():
            px_data_product = PXDataProduct(self, row)
            success = px_data_product.make_px()
            if success:
                write_px(px_data_product.list_of_lines, px_data_product.px_output_path)

