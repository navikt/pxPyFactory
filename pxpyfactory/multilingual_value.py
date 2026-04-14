
import pxpyfactory.validation


class MultilingualValue:
    def __init__(self):
        self._values = {}

    def set(self, value, language=None, append=False):
        if append:
            existing_value = self._values.get(language)
            if existing_value is not None:
                if not isinstance(existing_value, (list, tuple)):
                    existing_value = [existing_value]
                if isinstance(value, (list, tuple)):
                    existing_value.extend(value)
                else:
                    existing_value.append(value)
                value = existing_value

        self._values[language] = value

    def get(self, language=None, strictly_enforce_language=False):
        search_languages = [language]

        if not strictly_enforce_language:
            stored_languages = list(self._values.keys()).copy()
            if language in stored_languages:
                stored_languages.remove(language)
            if None in stored_languages: # Move None to the next position after the requested language, so that it is checked before other languages but after the requested language.
                stored_languages.remove(None)
                search_languages.append(None)
            search_languages.extend(stored_languages)

        for search_language in search_languages:
            return_candidate = self._values.get(search_language)
            if return_candidate is not None:
                return return_candidate

        return None
    

class MultilingualValueScope:
    def __init__(self, scope_name=None):
        self.name = MultilingualValue()
        self.value = MultilingualValue()
        self.set_name(scope_name=scope_name, language='raw')

    def set_name(self, scope_name, language=None, append=False):
        self.name.set(scope_name, language=language, append=append)

    def get_name(self, language=None, strictly_enforce_language=True, allow_empty_return_value=True):
        return self.name.get(language=language, strictly_enforce_language=strictly_enforce_language)

    def set_value(self, value, language=None, append=False):
        if value is not None:
            self.value.set(value, language=language, append=append)

    def get_value(self, language=None, strictly_enforce_language=True, allow_empty_return_value=True):
        return self.value.get(language=language, strictly_enforce_language=strictly_enforce_language)

    def update_translation(self, from_value, to_value, language=None):
        if language is None:
            # if language is None, we want to check all languages for both name and value except 'raw', and update all languages where the from_value is found
            name_languages_to_check = {lang for lang in self.name._values.keys() if lang != 'raw'}
            name_languages_to_check.add(None)
            value_languages_to_check = {lang for lang in self.value._values.keys() if lang != 'raw'}
            value_languages_to_check.add(None)
        else:
            name_languages_to_check = [language]
            value_languages_to_check = [language]

        for language in name_languages_to_check:
            for get_language in [language, None, 'raw']:
                current_name = self.get_name(language=get_language)
                if current_name is not None:
                    updated_name = self._replace_in_value(current_name, from_value, to_value)
                    if updated_name != current_name:
                        self.set_name(scope_name=updated_name, language=language)
                    break

        for language in value_languages_to_check:
            for get_language in [language, None, 'raw']:
                current_value = self.get_value(language=get_language)
                if current_value is not None:
                    updated_value = self._replace_in_value(current_value, from_value, to_value)
                    if updated_value != current_value:
                        self.set_value(value=updated_value, language=language)
                    break

    def _replace_in_value(self, value, from_value, to_value):
        if isinstance(value, list):
            updated_list = []
            for v in value:
                if v == from_value:
                    updated_list.append(to_value)
                else:
                    updated_list.append(v)
            return updated_list
        elif value == from_value:
            return to_value
        return value

