try:
    from shlex import quote as cmd_quote
except ImportError:
    from pipes import quote as cmd_quote

from urllib.parse import urljoin


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]


def quote_string(string_to_quote):
    return cmd_quote(string_to_quote)


def valid_dict_item(dict_to_check, item_key):
    if item_key not in dict_to_check or not dict_to_check[item_key]:
        return False
    return True


def url_join(url_base, path):
    return urljoin(url_base, path.lstrip('/'))


def dict_contains_keys(dict_to_check, keys):
    for key in keys:
        if key not in dict_to_check:
            return False
    return True
