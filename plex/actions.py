import requests
from loguru import logger

from utils import misc
from . import metadata


def refresh_item_metadata(cfg, metadata_item_id):
    try:
        plex_refresh_url = misc.urljoin(cfg.plex.url, f'/library/metadata/{metadata_item_id}/refresh')
        params = {
            'X-Plex-Token': cfg.plex.token
        }

        # send refresh request
        logger.debug(f"Sending refresh metadata request to: {plex_refresh_url}")
        resp = requests.put(plex_refresh_url, params=params, verify=False, timeout=30)

        logger.trace(f"Request URL: {resp.url}")
        logger.trace(f"Response: {resp.status_code} {resp.reason}")

        if resp.status_code != 200:
            logger.error(f"Failed refreshing metadata for item {metadata_item_id!r}: {resp.status_code} {resp.reason}")
            return False

        return True

    except Exception:
        logger.exception(f"Exception refreshing Plex metadata for item {metadata_item_id}: ")
    return False


def analyze_metadata_item(cfg, metadata_item_id):
    try:
        plex_analyze_url = misc.urljoin(cfg.plex.url, f'/library/metadata/{metadata_item_id}/analyze')
        params = {
            'X-Plex-Token': cfg.plex.token
        }

        # send refresh request
        logger.debug(f"Sending analyze metadata_item_id request to: {plex_analyze_url}")
        resp = requests.put(plex_analyze_url, params=params, verify=False, timeout=600)

        logger.trace(f"Request URL: {resp.url}")
        logger.trace(f"Response: {resp.status_code} {resp.reason}")

        if resp.status_code != 200:
            logger.error(f"Failed analyzing metadata_item {metadata_item_id!r}: {resp.status_code} {resp.reason}")
            return False

        return True
    except Exception:
        logger.exception(f"Exception analyzing Plex metadata_item_id {metadata_item_id}: ")
    return False


def set_metadata_item_collection(cfg, metadata_item_id, collection_name):
    try:
        # retrieve metadata_item_id details
        result = metadata.get_metadata_item_id(cfg.plex.database_path, metadata_item_id)
        if not result or not misc.dict_contains_keys(result, ['id', 'library_section_id', 'metadata_type']):
            logger.error(f"Unable to find metadata_item with id: {metadata_item_id!r}")
            return False

        # we have the details we need to build a metadata_item update
        plex_update_url = misc.urljoin(cfg.plex.url, f"/library/sections/{result['library_section_id']}/all")
        params = {
            'X-Plex-Token': cfg.plex.token,
            'id': metadata_item_id,
            'type': result['metadata_type'],
            'collection[0].tag.tag': collection_name,
            'includeExternalMedia': 1
        }

        # send update request
        logger.debug(f"Sending update metadata_item_id request to: {plex_update_url}")
        resp = requests.put(plex_update_url, params=params, verify=False, timeout=600)

        logger.trace(f"Request URL: {resp.url}")
        logger.trace(f"Response: {resp.status_code} {resp.reason}")

        if resp.status_code != 200:
            logger.error(f"Failed updating metadata_item {metadata_item_id!r}: {resp.status_code} {resp.reason}")
            return False

        return True

    except Exception:
        logger.exception(f"Exception updating metadata_item with id {metadata_item_id!r} to be in the "
                         f"{collection_name!r} collection: ")
    return False


def set_metadata_item_summary(cfg, metadata_item_id, summary):
    try:
        # retrieve metadata_item_id details
        result = metadata.get_metadata_item_id(cfg.plex.database_path, metadata_item_id)
        if not result or not misc.dict_contains_keys(result, ['id', 'library_section_id', 'metadata_type']):
            logger.error(f"Unable to find metadata_item with id: {metadata_item_id!r}")
            return False

        # we have the details we need to build a metadata_item update
        plex_update_url = misc.urljoin(cfg.plex.url, f"/library/sections/{result['library_section_id']}/all")
        params = {
            'X-Plex-Token': cfg.plex.token,
            'id': metadata_item_id,
            'type': result['metadata_type'],
            'summary.value': summary,
            'includeExternalMedia': 1
        }

        # send update request
        logger.debug(f"Sending update metadata_item_id request to: {plex_update_url}")
        resp = requests.put(plex_update_url, params=params, verify=False, timeout=600)

        logger.trace(f"Request URL: {resp.url}")
        logger.trace(f"Response: {resp.status_code} {resp.reason}")

        if resp.status_code != 200:
            logger.error(f"Failed updating metadata_item {metadata_item_id!r}: {resp.status_code} {resp.reason}")
            return False

        return True

    except Exception:
        logger.exception(f"Exception updating the summary of metadata_item with id {metadata_item_id!r}")
    return False


def set_metadata_item_poster(cfg, metadata_item_id, poster_url):
    try:
        plex_update_url = misc.urljoin(cfg.plex.url, f"/library/metadata/{metadata_item_id}/posters")
        params = {
            'X-Plex-Token': cfg.plex.token,
            'includeExternalMedia': 1,
            'url': poster_url
        }

        # send update request
        logger.debug(f"Sending update metadata_item_id request to {plex_update_url}")
        resp = requests.post(plex_update_url, params=params, verify=False, timeout=600)

        logger.trace(f"Request URL: {resp.url}")
        logger.trace(f"Response: {resp.status_code} {resp.reason}")

        if resp.status_code != 200:
            logger.error(f"Failed updating metadata_item {metadata_item_id!r}: {resp.status_code} {resp.reason}")
            return False

        return True

    except Exception:
        logger.exception(f"Exception updating poster for metadata_item with id {metadata_item_id!r} to {poster_url!r}:")
    return False
