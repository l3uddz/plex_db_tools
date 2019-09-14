import requests
from loguru import logger

from utils import misc


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
