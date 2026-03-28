class MultilingualValue:
    def __init__(self, default_language=None):
        self._values = {}
        self._default_language = default_language

    def set(self, value, language=None):
        self._values[language] = value

    def get(self, language=None, fallback=True):
        if language in self._values:
            return self._values[language]
        if fallback:
            if self._default_language in self._values:
                return self._values[self._default_language]
            if None in self._values:
                return self._values[None]
            for v in self._values.values():
                if v is not None:
                    return v
        return None

    def languages(self):
        return list(self._values.keys())

    def has_value(self, language=None):
        return language in self._values and self._values[language] is not None

    def __repr__(self):
        return f"MultilingualValue({self._values})"
