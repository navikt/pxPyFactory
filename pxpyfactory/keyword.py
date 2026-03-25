import re
import math
import pxpyfactory.helpers
import pxpyfactory.validation

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
                    allow_empty_return_value=False,
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
        self.mandatory                                      = self._interpret_boolean(mandatory, input_true_values={'yes', 'nav'})
        self.multiline                                      = self._interpret_boolean(multiline, input_true_values={'yes'})
        self.language_dependent                             = self._interpret_boolean(language_dependent)
        self.value_type, self.valid_values, self.max_length = self._derive_value_constraints(value_type, length)
        self.strictly_enforce_language                      = self._interpret_boolean(strictly_enforce_language)
        self.strictly_enforce_scope                         = self._interpret_boolean(strictly_enforce_scope)
        self.scope_can_not_be_both_none_and_specific        = self._interpret_boolean(scope_can_not_be_both_none_and_specific)
        self.allow_empty_return_value                       = self._interpret_boolean(allow_empty_return_value)
        self.use_default_value_as_base                      = self._interpret_boolean(use_default_value_as_base)
        self.default_value                                  = self._split_if_str_with_char(default_value, ';')
        
        self.value = {}
        # If a value is provided at initialization, it will be set using the set_value method to ensure it is properly coerced and stored in the correct format.
        # Value may be None, and added later using the set_value method.
        if value is not None:
            self.set_value(value)
    
    # # _____________________________________________________________________________
    # def __str__(self):
    #     return str(self.value)

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
    def set_value(self, value, language=None, scope=None, append=False, set_as_default_value=False):
        if not self.language_dependent:
            language = None  # Ignore language if not language-dependent
        if isinstance(value, dict):
            # Not supported yet...
            self._split_value(value, language, scope, append)
            return
        
        prepared_value = self._coerce_value(value)

        if set_as_default_value:
            self.default_value = prepared_value
            return

        if isinstance(scope, (list, tuple)):
            scope = '", "'.join(scope) # Convert list or tuple of scopes to a comma-separated string for consistent handling and storage.
        # Ensure the value dictionary is properly initialized for the given scope and language
        if scope not in self.value or not isinstance(self.value.get(scope), dict):
            self.value[scope] = {}

        if append:
            new_value = self.value[scope].get(language) # Get the existing value for the scope and language (which may be None, a single value, or a list of values).
            if not isinstance(new_value, list):
                new_value = [new_value] if new_value is not None else []
            if isinstance(prepared_value, list):
                new_value.extend(prepared_value)
            else:
                new_value.append(prepared_value)
        else:
            new_value = prepared_value

        self.value[scope][language] = new_value

    # _____________________________________________________________________________
    # This function is currently not in use,
    # but can be used in the future to set multiple values for different languages and scopes at once by passing a nested dictionary.
    # Possible format: {scope1: {language1: value, language2: value}, scope2: {language1: value, ...}, ...}.
    # The function will iterate through the nested dictionary and set the values accordingly using the set_value method.
    def _split_value(self, value_dict, language=None, scope=None, append=False):
        return
    
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
    def get_value(
        self,
        language=None,
        scope=None,
        strictly_enforce_language=None,
        strictly_enforce_scope=None,
        allow_empty_return_value=None, # This means that for example an empty string can be returned. Everytning that is not None is ok. It don't matter if it is considered valid or not.
    ):
        if not self.language_dependent:
            language = None  # Ignore language if not language-dependent
        if strictly_enforce_language is None:
            strictly_enforce_language = self.strictly_enforce_language
        if strictly_enforce_scope is None:
            strictly_enforce_scope = self.strictly_enforce_scope
        if allow_empty_return_value is None:
            allow_empty_return_value = self.allow_empty_return_value

        def pick_from_scope(scope_key):
            scope_values = self.value.get(scope_key)
            if not isinstance(scope_values, dict): # No values stored for this scope.
                return None

            return_candidate = scope_values.get(language)
            # If found value is a valid value for the specific scope and language it is a match.
            # If the found value is not valid, but allow_empty_return_value is True and the value is not None, we will return it anyway.
            if (allow_empty_return_value and return_candidate is not None) or pxpyfactory.validation.valid_value(return_candidate):
                return return_candidate

            if not strictly_enforce_language: # Search for any value in the same scope if specific language is not found or not valid.
                for return_candidate in scope_values.values():
                    if (allow_empty_return_value and return_candidate is not None) or pxpyfactory.validation.valid_value(return_candidate):
                        return return_candidate

            return None

        # 1) Try requested scope first
        value = pick_from_scope(scope)
        if value is not None:
            return value

        # 2) Fallback: search other scopes for a valid value
        if not strictly_enforce_scope and isinstance(self.value, dict) and self.value:
            other_scopes = [s for s in self.value.keys() if s != scope]

            # Put generic scope (None) first in the list of other scopes to check
            if None in other_scopes:
                other_scopes.remove(None)
                other_scopes.insert(0, None)

            for other_scope in other_scopes:
                value = pick_from_scope(other_scope)
                if value is not None:
                    return value

        return self.default_value

    # _____________________________________________________________________________
    # _____________________________________________________________________________
    # Returns the full value of the keyword in a format that can be directly written to the px file.
    # It may return several lines.
    def get_px_lines(self, languages=None, warn_on_missing_mandatory=False):
        if not self.language_dependent or languages is None:
            target_languages = [None]
        elif isinstance(languages, str):
            target_languages = [languages]
        else:
            target_languages = list(languages)

        lines = []
        if self.value: # If the value of the keyword is not empty / {}
            scopes_to_use = list(self.value.keys())
            if self.scope_can_not_be_both_none_and_specific and None in scopes_to_use and any(scope is not None for scope in scopes_to_use):
                scopes_to_use = [scope for scope in scopes_to_use if scope is not None] # Removes all None scopes if there are specific scopes provided.

            for scope in scopes_to_use:
                for language in target_languages:
                    value = self.get_value(language=language, scope=scope)
                    # For example CONTACT keyword includes default_value with additional values.
                    if self.use_default_value_as_base and (value != self.default_value) and (self.default_value is not None) and (value is not None):
                        value = self.default_value + value
                    lines.append(self._to_px_line(language=language, scope=scope, value=value))
        else:
            if self.mandatory: # If the keyword is mandatory, but the value is empty
                # we will return the keyword with an empty value or the default value if provided.
                for language in target_languages:
                    lines.append(self._to_px_line(language=language, scope=None, value=self.default_value))
                    if warn_on_missing_mandatory and self.default_value is None:
                        if language is None:
                            pxpyfactory.helpers.print_filter(f"WARNING: Mandatory keyword '{self.name}' is missing a value. Writing empty value in px file.", 1)
                        else:
                            pxpyfactory.helpers.print_filter(f"WARNING: Mandatory keyword '{self.name}' is missing a value for language '{language}'. Writing empty value in px file.", 1)
            # If the keyword is not mandatory, we will return an empty list, which will result in the keyword not being included in the output px file.
        return lines

    # _____________________________________________________________________________
    def _to_px_line(self, language, scope, value):
        line_str = self.name
        if language is not None:
            line_str += f"[{language}]"
        if scope is not None:
            line_str += f"(\"{scope}\")"
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
    