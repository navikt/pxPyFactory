import pxpyfactory.data_product
import pxpyfactory.file_io
import pxpyfactory.log
import pxpyfactory.deployment
import pxpyfactory.helpers
import pxpyfactory.main_praparation
import pandas as pd

class PXMain:
    def __init__(self):
        # Set folders and file-paths using config
        self.input_path = pxpyfactory.config.paths.INPUT
        self.output_path = pxpyfactory.config.paths.OUTPUT
        self.sq_output_path = pxpyfactory.config.paths.SAVED_QUERY_OUTPUT
        self.common_meta_filepath = pxpyfactory.config.paths.COMMON_METADATA_FILE
        self.production_log_filepath = pxpyfactory.config.paths.PRODUCTION_LOG_FILE

        self.data_products_df: pd.DataFrame = pd.DataFrame()
        self.keywords_base: dict = {}
        # self.translation_base: pd.DataFrame = pd.DataFrame()
        self.production_log: pxpyfactory.log.PXLog = None
        self.alias_df: pd.DataFrame = pd.DataFrame()
        self.mainprep_ok: bool = False

        self.px_files_written: int = 0
        self.px_files_written_ref: list[str] = []
        self.sq_file_pairs_written: int = 0
        self.deployment_needed: bool = False


    def run(self):

        self.mainprep()
        if not self.mainprep_ok:
            return
        ### Start processing each data product:
        for i, row in self.data_products_df.iterrows():
            self.process_data_product(i, row)

        self.log_and_deploy()

    def mainprep(self):
        self.mainprep_ok = False
        pxpyfactory.helpers.print_filter(f"--- Main input found: {pxpyfactory.file_io.file_exists(self.common_meta_filepath)} ---", 1)

        pxpyfactory.helpers.set_input_args() # Initialize input arguments from command line into helpers module (will be used several places later)
        # Check if there has been any changes to input files since last production
        if pxpyfactory.helpers.get_input_args('clean'):
            pxpyfactory.helpers.print_filter(f"--- Clean (delete all content in output folder before creating new structure) ---", 0)
            pxpyfactory.file_io.delete_content_in_path(self.output_path)    # Clean output folder before creating new structure
            pxpyfactory.file_io.delete_content_in_path(self.sq_output_path) # Clean saved query output folder before creating new structure
            self.deployment_needed = True
    
        self.production_log = pxpyfactory.log.PXLog(self, self.production_log_filepath)
        # Check if there has been any changes to input files since last production
        if not self.production_log.input_change() and pxpyfactory.helpers.get_input_args('build') is None:
            pxpyfactory.helpers.print_filter(f"--- Content in input folder has not changed since last run (exit)---", 0)
            return

        self.data_products_df = pxpyfactory.main_praparation.prepare_data_products(self.common_meta_filepath) # Get and prepare data products for px file generation from Excel-sheet.
        self.keywords_base = pxpyfactory.main_praparation.prepare_keywords_base(self.common_meta_filepath) # Get and prepare keywords base for px file generation from Excel-sheet.
        # self.translation_base = pxpyfactory.translation.prepare_translation(self.common_meta_filepath) # Get translation table for multi-language support

        # Check if there has been any changes to common meta since last production
        if self.production_log.common_meta_change() or pxpyfactory.helpers.get_input_args('build') == 'all':
            pxpyfactory.helpers.print_filter(f"--- Content in common meta has changed since last run (rebuild aliases and folders) ---", 1)
            self.alias_df = pxpyfactory.main_praparation.prepare_alias(self.common_meta_filepath) # Get and prepare alias
            pxpyfactory.main_praparation.update_folder_structure(self.data_products_df, self.alias_df, self.output_path) # Create folder structure from data_products dataframe
            self.production_log.alias_built = True
            self.deployment_needed = True
        else:
            pxpyfactory.helpers.print_filter(f"--- No changes in common meta since last run (skip rebuild of aliases and folders) ---", 1)

        self.mainprep_ok = True


    def process_data_product(self, process_number, product_definition):
        pxpyfactory.helpers.print_filter(f"--- Start processing data product / table from orderline: {process_number+2} ---", 1)
        px_data_product = pxpyfactory.data_product.PXDataProduct(self, product_definition)
        # Check if input files have changed since last production (Any need for rebuild of px for this table?)
        build_this_data_product = (
            px_data_product.force_build
            or self.production_log.data_product_change(px_data_product)
        )
        if not build_this_data_product:
            pxpyfactory.helpers.print_filter(f"INFO: No changes in input files since last run. Skipping this data product / table.", 1)
            return
        
        successfull_make = px_data_product.create_px_content()
        if not successfull_make:
            pxpyfactory.helpers.print_filter(f"ERROR: Failed to create PX content for table '{px_data_product.tableid}' (skip writing px file)", 0)
            return
        pxpyfactory.helpers.print_filter(f"PX content successfully created for table '{px_data_product.tableid}'", 1)
        
        sucessfull_write = pxpyfactory.file_io.file_write(px_data_product.px_output_path, "\n".join(px_data_product.list_of_lines))
        if not sucessfull_write: # If writing px file to disk is successful, log the production
            pxpyfactory.helpers.print_filter(f"ERROR: Failed to write PX file for table '{px_data_product.tableid}'", 0)
            return
        pxpyfactory.helpers.print_filter(f"PX file successfully written: {px_data_product.px_output_path}", 1)
        self.production_log.log_data_product(px_data_product)
        self.px_files_written += 1
        self.px_files_written_ref.append(px_data_product.tableid)
        self.deployment_needed = True

        successfull_sq = px_data_product.make_sq() # Create standard Saved Query for the px file
        if not successfull_sq:
            pxpyfactory.helpers.print_filter("ERROR: Failed to create Saved Query files", 0)
            return
        pxpyfactory.helpers.print_filter("Saved Query files generated", 1)
        self.sq_file_pairs_written += 1


    def log_and_deploy(self):
        # After processing all data products, print summary and trigger deployment if not in test mode
        if pxpyfactory.helpers.get_input_args('test') or pxpyfactory.helpers.get_input_args('test_full'):
            pxpyfactory.helpers.print_filter(f"--- PX file generation test finished.", 0)
            return

        pxpyfactory.helpers.print_filter(f"--- PX file generation completed. Total PX files written: {self.px_files_written} (included saved query file pairs: {self.sq_file_pairs_written}) ---", 0)
        if len(self.px_files_written_ref) > 0:
            pxpyfactory.helpers.print_filter(f"PX files written for these tables: {', '.join(self.px_files_written_ref)}", 0)

        # Log successful productions to production_log with list of tables built
        self.production_log.write_log(self.px_files_written_ref)

        if (self.deployment_needed and not pxpyfactory.helpers.get_input_args('no_deploy')) or pxpyfactory.helpers.get_input_args('deploy'):
            environment = pxpyfactory.helpers.get_input_args('environment')
            branch = pxpyfactory.helpers.get_input_args('branch')
            pxpyfactory.deployment.trigger_deployment(environment, branch)



