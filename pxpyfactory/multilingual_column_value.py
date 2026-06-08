
class MultilingualColumnValue:

    @staticmethod
    def prepare_language_columns(table_data, keyword_language='no', keyword_languages=None):
        language_preference_order = [keyword_language] + [language for language in (keyword_languages or []) if language != keyword_language]
        
        language_columns_mapping = {} # Mapping of column_name -> base_value -> language -> translation, e.g., {'FYLKE': {'Oslo': {'en': 'Oslo County', 'no': 'Oslo'}}}
        base_columns = {}
        columns_to_drop = set()

        for column_name in table_data.columns:
            # If the column name ends with a pattern like __EN, __NO, etc.. it is considered a language-specific column,
            # and the base column name is extracted by removing the language suffix, and the language code is extracted from the suffix.
            # For example, for a column named 'FYLKE__EN', the base column name would be 'FYLKE' and the language code would be 'en'.
            language = None
            if column_name[-4:-2] == "__" and column_name[-2:].isalpha():
                column_name_base = column_name[:-4]
                language = column_name[-2:].lower()
                columns_to_drop.add(column_name)
            else:
                column_name_base = column_name
            if column_name_base not in base_columns:
                base_columns[column_name_base] = {}
            base_columns[column_name_base][language] = column_name

        for base_column in base_columns:
            # If the base column (default language column) does not exist in the table, create it by taking the first available language-specific column
            #   based on the defined language preference order, to ensure there is a fallback column for each set of language-specific columns.
            if base_columns[base_column].get(None) is None:
                for preferred_language in language_preference_order:
                    if preferred_language in base_columns[base_column]:
                        table_data[base_column] = table_data[base_columns[base_column][preferred_language]]
                        base_columns[base_column][None] = base_columns[base_column][preferred_language]
                        break

            values = {}
            for value in table_data[base_column].unique():
                # Find all translations for value in base column to values in language-specific column.
                rows_with_value = table_data.loc[table_data[base_column] == value]
                for language, column_name in base_columns[base_column].items():
                    if values.get(language) is None:
                        values[language] = []
                    language_specific_values = rows_with_value[column_name]
                    # Remove NaN and empty values from the language_specific_values Series and convert to string for consistent processing
                    language_specific_values = language_specific_values.dropna().apply(lambda value: str(value).strip())
                    language_specific_values = language_specific_values[language_specific_values != '']
                    if language_specific_values.empty: # There is no valid translation for this value in this language.
                        values[language].append(value) # Use the value from the base column as fallback
                    else:
                        values[language].append(language_specific_values.iloc[0])
            language_columns_mapping[base_column] = values

        # Remove language-specific columns from the table data to clean up the DataFrame,
        # as the translations are now stored in the mapping and fallback columns are created for use.
        table_data = table_data.drop(columns=columns_to_drop)
        return table_data, language_columns_mapping

# ----------------------------
