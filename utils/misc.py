try:
    from shlex import quote as cmd_quote
except ImportError:
    from pipes import quote as cmd_quote

import json
from urllib.parse import urljoin

from dateutil.parser import parse
from loguru import logger


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


def load_settings(json_path):
    try:
        with open(json_path, 'r') as fp:
            return json.load(fp)
    except Exception:
        return {}


def dump_settings(json_path, settings):
    try:
        with open(json_path, 'w') as fp:
            json.dump(settings, fp, indent=2)
        return True
    except Exception:
        logger.exception(f"Exception dumping settings to {json_path!r}: ")
    return False


def is_utc_timestamp_before(timestamp_to_check, timestamp_to_check_against):
    try:
        to_check = parse(timestamp_to_check)
        check_against = parse(timestamp_to_check_against)
        if to_check > check_against:
            return True
    except Exception:
        logger.exception(f"Exception comparing timestamp {timestamp_to_check} against {timestamp_to_check_against}: ")
    return False
