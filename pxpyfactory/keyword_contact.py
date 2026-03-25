import re

def shape_to_px(value, type_separator=None):
    header = ''
    body = ''
    phone = ''
    email = ''
    empty = ''
    px_type_separator = '#'
    px_extra_separator = '||'

    if type_separator is None:
        type_separator = px_type_separator
    if isinstance(value, str):
        value = [part.strip() for part in value.split(type_separator)]
    if isinstance(value, (list, tuple)):
        for item in value:
            item_str = str(item).strip()
            if _looks_like_email(item_str):
                email = item_str
            elif _looks_like_phone(item_str):
                phone = item_str
            else:
                if header == '':
                    header = item_str
                else:
                    body += item_str
        value = [str(v).strip() for v in value]
    
    px_str = px_type_separator.join([header, empty, empty, phone, email, body, empty, px_extra_separator])
    
    return px_str # Shaped contact value should have this format: 'Nav Statistikk###+47 12345678#nav.statistikk@nav.no###||'


def _looks_like_email(value: str):
    email_pattern = r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$"
    return bool(re.match(email_pattern, value))


def _looks_like_phone(value: str):
    normalized = re.sub(r"[\s().-]", "", value)
    if normalized.startswith("+"):
        normalized = normalized[1:]
    return normalized.isdigit() and 5 <= len(normalized) <= 15


