import re
import math
import pxpyfactory.helpers
import pxpyfactory.validation
import pxpyfactory.multilingual_value

# This class will instansiate each keyword in the final px-file.
# All language data will be handled/stored within this class.
# Initial names (for example column names in input table) is stored with language='raw'.
# ? Need for linking keywords together ?

class Keyword:
    def __init__(   self,
                    name,
                    order=500,
                    mandatory=False,
                    language_dependent=False,
                    value_type=str,
                    length=None,
                    multiline=False,
                    strictly_enforce_language=False,
                    strictly_enforce_scope=True, 
                    scope_can_not_be_both_none_and_specific=True, # If True and specific scope is provided, None scope is just fallback and not included from get_px_lines().
                    use_value_from_none_scope_if_specific_scope_has_no_value=True,
                    allow_empty_return_value=False,
                    set_value_use_append=False, # If True, when set_value is called multiple times for the same scope and language, the values will be appended to a list instead of overwriting each other.
                    use_default_value_as_base=False,
                    default_value=None,
                    value=None,
    ):
        
        self.name = name

        try:
            order = int(order)
        except (TypeError, ValueError):
            raise ValueError(f"Invalid order value: {order}. Order must be an integer.")
        self.order = order

        # If mandatory is any of these strings the keyword will be considered mandatory, and always included in the output px file.
        # If no value is set for a mandatory keyword, the default value will be used if provided, otherwise an empty value will be written in the px file.
        self.mandatory                                                = self._interpret_boolean(mandatory, input_true_values={'yes', 'nav'})
        self.multiline                                                = self._interpret_boolean(multiline, input_true_values={'yes'})
        self.language_dependent                                       = self._interpret_boolean(language_dependent)
        self.value_type, self.valid_values, self.max_length           = self._derive_value_constraints(value_type, length)
        self.strictly_enforce_language                                = self._interpret_boolean(strictly_enforce_language)
        self.strictly_enforce_scope                                   = self._interpret_boolean(strictly_enforce_scope)
        self.scope_can_not_be_both_none_and_specific                  = self._interpret_boolean(scope_can_not_be_both_none_and_specific)
        self.use_value_from_none_scope_if_specific_scope_has_no_value = self._interpret_boolean(use_value_from_none_scope_if_specific_scope_has_no_value)
        self.allow_empty_return_value                                 = self._interpret_boolean(allow_empty_return_value)
        self.set_value_use_append                                     = self._interpret_boolean(set_value_use_append)
        self.use_default_value_as_base                                = self._interpret_boolean(use_default_value_as_base)
        self.default_value                                            = self._split_if_str_with_char(default_value, ';')
        
        # Each keyword can have from 0 to many scopes. If the keyword do not have any scopes, the value will be stored with scope=None.
        # Each scope have one name and one value for 0 to many languages each. If the keyword do not have any scopes, the name of the None scope is irrelevant/not in use.
        # Name and value is stored in MultilingualValue objects. Thus two of these objects are used for each scope.
        # If langauge_dependent is False, MultilingualValue objects only store one value with language=None.

        self.scope_refs = [] # List of scopes for the keyword.
        # Each scope is a MultilingualValueScope.
        # A MultilingualValueScope contain two MultilingualValue objects,
        # one for the name of the scope and one for the value of the scope.

        # If a value is provided at initialization, it will be set using the set_value method to ensure it is properly coerced and stored in the correct format.
        # Value may be None, and added later using the set_value method.
        if value is not None:
            self.set_value(value)
    
    # _____________________________________________________________________________
    def _interpret_boolean(self, value, input_true_values={'yes', 'true', '1', 'ja'}, input_false_values={'no', 'false', '0', 'nei'}, none_is_false=True):
        if value is None:
            return False if none_is_false else None

        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value > 0
        if isinstance(value, str):
            lower_value = value.strip().lower()
            if lower_value in input_true_values:
                return True
            elif lower_value in input_false_values:
                return False
        return False

    # _____________________________________________________________________________
    def _derive_value_constraints(self, value_type, length):
        valid_values = None
        max_length = None

        # Update value_type. Valid values will be set if value_type is provided in the format "{value1,value2,...}".
        if value_type not in (str, int, bool):
            if isinstance(value_type, str):
                value_type = value_type.strip()
                value_type_normalized = value_type.replace(' ', '').lower()

                if value_type_normalized in ['text', 'str']:
                    value_type = str
                elif value_type_normalized in ['integer', 'int']:
                    value_type = int
                elif value_type_normalized in ['boolean', 'bool', '{yes,no}', '{no,yes}']:
                    value_type = bool
                elif '{' in value_type and '}' in value_type and ',' in value_type:
                    # If the value_type is a list of valid values in the format "{value1,value2,...}",
                    # we will extract those values and store them as a tuple in valid_values, and set the value_type to str.
                    valid_values_str = re.findall(r"\{(.*?)\}", value_type)
                    if valid_values_str:
                        valid_values = tuple(value.strip() for value in valid_values_str[0].split(','))
                        value_type = str
                    else:
                        raise ValueError(f"Invalid value_type format: {value_type}. Expected format for list of valid values is '{{value1,value2,...}}'.")
                else:
                    value_type = str
            else:
                value_type = str

        # Handle length parameter to set valid_values or max_length
        if length is not None and isinstance(length, str) and length.strip() != '':
            try:
                numbers_in_length = re.findall(r"\d+", length)
                int_first = int(numbers_in_length[0]) if len(numbers_in_length) >= 1 else None
                int_second = int(numbers_in_length[1]) if len(numbers_in_length) >= 2 else None
            except ValueError:
                raise ValueError(f"Invalid length value: {length}. Length must contain an integer, a list of integers, or None.")
            if int_second is not None and valid_values is None: # If there are two numbers and valid_values is not set above, create a complete tuple of valid values in the interval.
                range_start, range_end = sorted((int_first, int_second))
                valid_values = tuple(range(range_start, range_end + 1)) # +1 because range end is exclusive, but we want to include it in the valid values.
            elif int_first is not None: # If there is only one number, we will consider that as the maximum length of the string in characters.
                max_length = int(int_first)

        return value_type, valid_values, max_length

    # _____________________________________________________________________________
    # Handle strings with the splitting char as a list of multiple values.
    def _split_if_str_with_char(self, input_value, char):
        if isinstance(input_value, str) and char in input_value:
            return self._coerce_value(input_value.split(char))
        return self._coerce_value(input_value)
    
    # _____________________________________________________________________________
    # _____________________________________________________________________________
    # This function is used to update the scope (name and value) for the given language of the keyword if the provided column matches the scope (or a part of the scope).
    # It is used ut update interconnected keywords.
    def update_columns(self, column=None, value=None, language=None):
        # Next line is just to be able to stop at this ponint when debugging.
        if self.name in ['TIMEVAL'] and column in ['STAT_VAR', 'AAR_KVARTAL']: # and language == 'en':
            pass
        for scope_ref in self.scope_refs:
            scope_ref.update_translation(from_value=column, to_value=value, language=language)

    # _____________________________________________________________________________
    # Return the scope or scopes that match the input scope.
    # Input scope may be a string or a list of strings.
    # If the input scope is a list of strings, it will be considered a match if
    # all of the strings in the list matches the scope or is included in the scope.
    def get_scope_ref(self, scope_name_to_match=None, only_exact_match=False, create_if_not_exist=False):
        matches = []
        matches_partly = []
        for scope_ref in self.scope_refs:
            raw_scope_name = scope_ref.get_name(language='raw', strictly_enforce_language=True, allow_empty_return_value=True)
            if raw_scope_name is None:
                if scope_name_to_match is None:
                    matches.append(scope_ref)
            else:
                if scope_name_to_match == raw_scope_name:
                    matches.append(scope_ref)
                elif isinstance(raw_scope_name, list):
                    if scope_name_to_match in raw_scope_name:
                        matches_partly.append(scope_ref)
        if not only_exact_match:
            matches.extend(matches_partly)

        if create_if_not_exist and (len(matches) == 0):
            # If there is no existing scope that matches the input scope, we will create a new scope.
            new_scope_ref = pxpyfactory.multilingual_value.MultilingualValueScope(scope_name=scope_name_to_match)
            self.scope_refs.append(new_scope_ref)
            matches = [new_scope_ref]

        return matches
    # _____________________________________________________________________________
    # Scope is the showed text in different languages.
    # Input scope may be a string or a list of strings.
    def set_scope(self, value=None, language=None, scope_name=None):
        if not self.language_dependent:
            language = None  # Ignore language if not language-dependent

        matched_scopes = self.get_scope_ref(scope_name_to_match=scope_name, create_if_not_exist=True)
        for matched_scope in matched_scopes:
            matched_scope.set_name(scope_name=scope_name, language=language, append=False)

    # _____________________________________________________________________________
    def set_value(self, value, language=None, scope_name=None, append=None, set_as_default_value=False):
        if not self.language_dependent:
            language = None  # Ignore language if not language-dependent
        if append is None:
            append = self.set_value_use_append

        # # _split_value is currently not in use,
        # # but can be used in the future to set multiple values for different languages and scopes at once by passing a nested dictionary.
        # # Possible format: {scope1: {language1: value, language2: value}, scope2: {language1: value, ...}, ...}.
        # # The function will iterate through the nested dictionary and set the values accordingly using the set_value method.
        # def _split_value(self, value_dict, language=None, scope=None, append=False):
        #     return
        
        # if isinstance(value, dict):
        #     # Not supported yet...
        #     _split_value(value, language, scope, append)
        #     return
        
        prepared_value = self._coerce_value(value)

        if set_as_default_value:
            self.default_value = prepared_value
            return

        matched_scopes = self.get_scope_ref(scope_name_to_match=scope_name, create_if_not_exist=True)
        for matched_scope in matched_scopes:
            matched_scope.set_value(value=prepared_value, language=language, append=append)
    
    # _____________________________________________________________________________
    # This method will coerce the input value to the correct type based on the value_type of the keyword.
    # It has special handling for boolean values, allowing for common string representations of true and false.
    def _coerce_value(self, input_value):
        if input_value is None:
            return None
        if isinstance(input_value, float) and math.isnan(input_value):
            return None
        if isinstance(input_value, (list, tuple)):
            return [self._coerce_value(item) for item in input_value]
        value_type = self.value_type
        try:
            if value_type is bool:
                return self._interpret_boolean(input_value)
            if value_type is str:
                return str(input_value).strip()
            return value_type(input_value)
        except Exception as error:
            value_type_name = getattr(value_type, '__name__', str(value_type))
            raise TypeError(f"Invalid value '{input_value}' for value_type={value_type_name}") from error

    # _____________________________________________________________________________
    # _____________________________________________________________________________
    def _get_name(self, language=None, scope_name=None, only_exact_match=True, strictly_enforce_language=None):
        if not self.language_dependent:
            language = None  # Ignore language if not language-dependent
        if strictly_enforce_language is None:
            strictly_enforce_language = self.strictly_enforce_language

        matched_scopes = self.get_scope_ref(scope_name_to_match=scope_name, create_if_not_exist=False)
        if len(matched_scopes) == 0:
            return None
        matched_scope_name = matched_scopes[0].get_name(language=language, strictly_enforce_language=strictly_enforce_language, allow_empty_return_value=True)

        return matched_scope_name # if pxpyfactory.validation.valid_value(matched_scope_name) else None
    # _____________________________________________________________________________
    def get_value(self, language=None, scope_name=None, strictly_enforce_language=None, strictly_enforce_scope=None, allow_empty_return_value=None):
        if not self.language_dependent:
            language = None  # Ignore language if not language-dependent
        if strictly_enforce_language is None:
            strictly_enforce_language = self.strictly_enforce_language
        if strictly_enforce_scope is None:
            strictly_enforce_scope = self.strictly_enforce_scope
        if allow_empty_return_value is None:
            allow_empty_return_value = self.allow_empty_return_value

        matched_scopes = self.get_scope_ref(scope_name_to_match=scope_name, create_if_not_exist=False)
        # if len(matched_scopes) == 0:
        #     pxpyfactory.helpers.print_filter(f"WARNING: No matching scope found for scope='{scope_name}' in keyword '{self.name}'. Returning None.", 1)
        #     return None

        if strictly_enforce_scope:
            # Only try requested scope
            matched_scopes = matched_scopes[:1]
        else:
            # Also try all other scopes in keyword. Startintg with the matched scopes, and then extending with other scopes if strictly_enforce_scope is False.
            matched_scopes.extend([scope_ref for scope_ref in self.scope_refs if scope_ref not in matched_scopes])
            # Put generic scope (None) first in the list of other scopes to check - Is this nessassary?
            # if None in matched_scopes: ### ingen er None, det er name som er None. 
            #     matched_scopes.remove(None)
            #     matched_scopes.insert(1, None)

        for matched_scope in matched_scopes:
            value = matched_scope.get_value(language=language, strictly_enforce_language=strictly_enforce_language, allow_empty_return_value=True)
            if value is not None:
                return value
        
        return self.default_value # Return default value if no value is found.

    # _____________________________________________________________________________
    # _____________________________________________________________________________
    # Returns the full value of the keyword in a format that can be directly written to the px file.
    # It may return several lines.
    def get_px_lines(self, languages=None, main_language=None, warn_on_missing_mandatory=False):
        if not self.language_dependent or languages is None:
            target_languages = [None]
        elif isinstance(languages, str):
            target_languages = [languages]
        else:
            target_languages = list(languages)

        lines = []

        scope_refs_to_use = self.scope_refs.copy()
        if len(scope_refs_to_use) > 0:
            scope_none_refs = self.get_scope_ref(scope_name_to_match=None, only_exact_match=True, create_if_not_exist=False)

            if self.scope_can_not_be_both_none_and_specific and (len(scope_none_refs) > 0) and (len(scope_none_refs) != len(scope_refs_to_use)):
                for scope_ref in scope_none_refs:
                    scope_refs_to_use.remove(scope_ref)

            for scope_ref in scope_refs_to_use:
                for language in target_languages:
                    value = scope_ref.get_value(language=language, strictly_enforce_language=self.strictly_enforce_language)
                    if value is None and self.use_value_from_none_scope_if_specific_scope_has_no_value:
                        for scope_none_ref in scope_none_refs:
                            value = scope_none_ref.get_value(language=language, strictly_enforce_language=self.strictly_enforce_language)
                            if value is not None:
                                break # Only use the first None scope that has a value, if there are multiple None scopes.
                    scope_name = scope_ref.get_name(language=language, strictly_enforce_language=self.strictly_enforce_language)
                    if self.use_default_value_as_base or self.set_value_use_append:
                        value = self._merge_value(value)
                    lines.append(self._to_px_line(language=language, main_language=main_language, scope_name=scope_name, value=value))
        else:
            if self.mandatory: # If the keyword is mandatory, but the value is empty
                # we will return the keyword with an empty value or the default value if provided.
                for language in target_languages:
                    lines.append(self._to_px_line(language=language, main_language=main_language, scope_name=None, value=self.default_value))
                    if warn_on_missing_mandatory and self.default_value is None:
                        if language is None:
                            pxpyfactory.helpers.print_filter(f"WARNING: Mandatory keyword '{self.name}' is missing a value. Writing empty value in px file.", 1)
                        else:
                            pxpyfactory.helpers.print_filter(f"WARNING: Mandatory keyword '{self.name}' is missing a value for language '{language}'. Writing empty value in px file.", 1)
            # If the keyword is not mandatory, we will return an empty list, which will result in the keyword not being included in the output px file.
        return lines

    # _____________________________________________________________________________
    # If the value is a list of values (due to append), we will merge it into a single string.
    # If use_default_value_as_base is True, default value will be merged with value.
    def _merge_value(self, value, separator=' '):
        value_is_list = isinstance(value, (list, tuple))
        if value_is_list:
            # Remove None and empty values from the list, and convert all values to stripped strings.
            value = [str(v).strip() for v in value if v is not None and str(v).strip() != '']
        # For example CONTACT keyword includes default_value with additional values.
        if self.use_default_value_as_base and (self.default_value is not None) and (value is not None):
            if value_is_list and (self.default_value not in value):
                return separator.join([self.default_value] + list(value))
            if isinstance(value, str) and self.default_value != value:
                return separator.join([self.default_value] + [value])
        if value_is_list:
            return separator.join(value)
        return value
    
    # _____________________________________________________________________________
    def _to_px_line(self, language, main_language, scope_name, value):
        line_str = self.name
        if (language is not None) and (language != main_language): # main_language may be written without language tag in the px file.
            line_str += f"[{language}]"
        if scope_name is not None:
            if isinstance(scope_name, (list, tuple)):
                scope_str = ", ".join(self._quote_px(item, value_type=str) for item in scope_name)
            else:
                scope_str = self._quote_px(scope_name, value_type=str)
            line_str += f"({scope_str})"
        if isinstance(value, (list, tuple)):
            value_str = ", ".join(self._quote_px(item) for item in value)
        else:
            value_str = self._quote_px(value)
        
        return f"{line_str}={value_str};"

    # _____________________________________________________________________________
    def _quote_px(self, value, value_type=None):
        if value_type is None:
            value_type = self.value_type
        if value_type is int:
            if value is None:
                return ""
            return str(value)
        if value is None:
            return "\"\""
        
        # Spesific handling of TLIST in TIMEVAL keyword, which should not be quoted in the px file, even though it is a string.
        if self.name == "TIMEVAL" and isinstance(value, str) and value.startswith("TLIST("):
            return f"{str(value)}"

        return f"\"{str(value)}\""
    