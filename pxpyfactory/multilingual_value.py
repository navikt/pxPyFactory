
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

    # def set_value(self, value, language=None, append=False):
    #     if append:
    #         existing_value = self._value.get_exact(language=language)
    #         if not isinstance(existing_value, list):
    #             existing_value = [existing_value] if existing_value is not None else []
    #         if isinstance(value, list):
    #             existing_value.extend(value)
    #         else:
    #             existing_value.append(value)
    #         self._value.set(existing_value, language=language)
    #         return

    #     self._value.set(value, language=language)

    # def get_value(self, language=None, strictly_enforce_language=False, allow_empty_return_value=False):
    #     return self._value.get(
    #         language=language,
    #         strictly_enforce_language=strictly_enforce_language,
    #         allow_empty_return_value=allow_empty_return_value,
    #     )

    # def set_scope_value(self, value, language=None):
    #     self._scope_value.set(value, language=language)

    # def get_scope_value(self, language=None, strictly_enforce_language=False, allow_empty_return_value=False):
    #     return self._scope_value.get(
    #         language=language,
    #         strictly_enforce_language=strictly_enforce_language,
    #         allow_empty_return_value=allow_empty_return_value,
    #     )
