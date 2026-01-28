import pxpyfactory.data_product
import pxpyfactory.utils
import pxpyfactory.io_utils
import pxpyfactory.log

class PXMain:
    def __init__(self):
        # Set folders and file-paths
        # script_path = set_script_path()
        self.input_path              = 'input' # Define input path relative to script location
        self.output_path             = 'px'  # Define output path relative to script location
        self.common_meta_filepath    = pxpyfactory.io_utils.get_path([self.input_path, 'common_meta.xlsx']) # Define path to common metadata file
        self.production_log_filepath = pxpyfactory.io_utils.get_path([self.input_path, 'production_log.jsonl']) # Define path to common metadata file

    def run(self):
        pxpyfactory.utils.print_filter(f"\n--- Main input found: {pxpyfactory.io_utils.file_exists(self.common_meta_filepath)} ---", 1)
        self.production_log = pxpyfactory.log.PXLog(self, self.production_log_filepath)

        # Check if there has been any changes to input files since last production
        if not self.production_log.input_change() and pxpyfactory.utils.force_build_arg() == None:
            pxpyfactory.utils.print_filter(f"--- Content in input folder has not changed since last run (exit)---", 0)
            return

        data_products_df    = pxpyfactory.utils.prepare_data_products(self.common_meta_filepath) # Get and prepare data products for px file generation from Excel-sheet.
        self.metadata_base  = pxpyfactory.utils.prepare_metadata_base(self.common_meta_filepath) # Get and prepare metadata_base

        # Check if there has been any changes to common meta since last production
        if self.production_log.common_meta_change():
            pxpyfactory.utils.print_filter(f"--- Content in common meta has changed since last run (rebuild aliases and folders) ---", 1)
            self.alias_df       = pxpyfactory.utils.prepare_alias(self.common_meta_filepath) # Get and prepare alias
            pxpyfactory.utils.update_folder_structure(data_products_df, self.alias_df, self.output_path) # Create folder structure from data_products dataframe
        else:
            pxpyfactory.utils.print_filter(f"--- No changes in common meta since last run (skip rebuild of aliases and folders) ---", 1)

        px_files_written = 0
        px_files_written_ref = []
        sq_file_pairs_written = 0
        # Process each data product:
        for i, row in data_products_df.iterrows():
            pxpyfactory.utils.print_filter(f"\n--- Start processing data product / table from orderline: {i+2} ---", 1)
            px_data_product = pxpyfactory.data_product.PXDataProduct(self, row)
            # Check if input files have changed since last production (Any need for rebuild of px for this table?)
            input_changed = self.production_log.data_product_change(px_data_product)
            if input_changed or px_data_product.force_build == True:
                if px_data_product.make_px():
                    # If px content was created successfully, continue to write px file
                    if pxpyfactory.io_utils.write_px(px_data_product.list_of_lines, px_data_product.px_output_path):
                        # If writing px file to disk is successful, log the production
                        self.production_log.log_data_product(px_data_product)
                        # If logging is successful, print confirmation
                        pxpyfactory.utils.print_filter(f"PX file successfully written: {px_data_product.px_output_path}", 1)
                        px_files_written += 1
                        px_files_written_ref.append(px_data_product.table_ref)
                        if px_data_product.make_sq(): # Create a standard Saved Query for the px file
                            pxpyfactory.utils.print_filter("Saved Query files generated", 1)
                            sq_file_pairs_written += 1
            else:
                pxpyfactory.utils.print_filter(f"INFO: No changes in input files since last run. Skipping this data product / table.", 1)

        pxpyfactory.utils.print_filter(f"\n--- PX file generation completed. Total PX files written: {px_files_written} (included saved query file pairs: {sq_file_pairs_written}) ---", 0)
        if len(px_files_written_ref) > 0:
            pxpyfactory.utils.print_filter(f"PX files written for these tables: {', '.join(px_files_written_ref)}", 0)
        # Log successful productions to production_log with list of tables built
        self.production_log.write_log(px_files_written_ref)
