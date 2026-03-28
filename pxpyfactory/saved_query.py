from datetime import datetime, timezone
import json
import pxpyfactory.helpers


class SavedQueryGenerator:
    """
    Generates saved query files (.sqa and .sqs) for PX data products.
    """
    
    def __init__(self, data_product):
        """
        Initialize with data from a PXDataProduct instance.
        
        Args:
            data_product: PXDataProduct instance containing table metadata and values
        """
        self.table_id = data_product.tableid
        self.table_meta_sq = data_product.table_meta_sq.copy()
        # self.rename_map = data_product.rename_map
        self.stub_list = data_product.stub_list
        self.heading_list = data_product.heading_list
        self.data_list = data_product.data_list
        self.values_dict = data_product.values_dict
        self.contvariable = data_product.contvariable
        self.language = "no"
    
    def generate_sqs(self):
        """
        Creates content for a .sqs (saved query statistics) file.
        
        Returns:
            JSON string for .sqs file
        """
        current_time = datetime.now(timezone.utc).isoformat()
        sqs_structure = {
            "Created": current_time,
            "LastUsed": current_time,
            "UsageCount": 1
        }
        return json.dumps(sqs_structure, indent=4, ensure_ascii=False)
    
    def generate_sqa(self):
        """
        Creates content for a .sqa (saved query attributes) file.
        
        Returns:
            JSON string for .sqa file
        """
        # # Apply rename map to SQ parameters
        # self.table_meta_sq['KEYWORD'] = self.table_meta_sq['KEYWORD'].map(
        #     lambda x: self.rename_map.get(x, x)
        # )
        
        # Build selection array with all variables
        selection = []
        all_variables = [self.contvariable] + self.heading_list + self.stub_list
        
        total_cells = 1
        for var in all_variables:
            if var == self.contvariable:
                value_count = len(self.data_list)
            else:
                value_count = len(self.values_dict[var])
     
            sq_meta_value = self.table_meta_sq.loc[self.table_meta_sq['KEYWORD'] == var, 'VALUE']
            if sq_meta_value.empty:
                sq_meta_value = None
            else:
                sq_meta_value = str(sq_meta_value.iloc[0]).strip() # only use first value found in table_meta_sq

            constraint_from_top = True
            try:
                value_constraint = int(sq_meta_value)
                if value_constraint < 0:
                    constraint_from_top = False
                    value_constraint = abs(value_constraint)
                value_constraint = min(value_constraint, value_count)
            except (IndexError, TypeError, ValueError):
                value_constraint = value_count
            
            # If number of cells exceeds maximum viewable cells in pxWeb2, reduce with hard limit
            if total_cells * value_constraint > pxpyfactory.config.defaults.MAX_SQ_CELLS or value_constraint == 0:
                value_constraint = 1
            else:
                total_cells *= value_constraint
            
            pxpyfactory.helpers.print_filter(
                f"sq: column {var} has {value_count} values, and it set to show "
                f"{'first' if constraint_from_top else 'last'} {value_constraint} values.", 3
            )
            
            # Take first or last N values
            if constraint_from_top:
                selected_indices = list(range(0, value_constraint))
            else:
                selected_indices = list(range(value_count - value_constraint, value_count))
            
            value_codes = [str(i) for i in selected_indices]
            
            selection.append({
                "VariableCode": var,
                "CodeList": None,
                "ValueCodes": value_codes
            })
        
        # Build the complete structure
        sqa_structure = {
            "Id": "",
            "Selection": {
                "Selection": selection,
                "Placement": {
                    "Heading": [self.contvariable] + self.heading_list,
                    "Stub": self.stub_list
                }
            },
            "Language": self.language,
            "TableId": self.table_id,
            "OutputFormat": 2,
            "OutputFormatParams": []
        }
        
        return json.dumps(sqa_structure, indent=4, ensure_ascii=False)
