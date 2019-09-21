import pandas as pd
from loguru import logger

from . import misc
from . import themoviedb

SHEETS_URL = 'https://docs.google.com/spreadsheets/d/e/' \
             '2PACX-1vTXDpwSDxKxWHNEfYSqnlaC_GVxzVavu7iPuAnEa_7LEGzhiQS29fD_1tplegJvljE5Zy1MB63umLzk/pub?output=csv'


def get_sheets_collection(id):
    logger.debug(f"Retrieving collection from sheets with id: {id!r}")
    try:
        # open sheet
        collections = pd.read_csv(SHEETS_URL, index_col=0)

        # parse item
        item = collections.loc[int(id), :]
        collection_name = str(item[0])
        collection_poster = str(item[1])
        collection_summary = str(item[2])
        collection_parts = str(item[3]).split(',')
        logger.debug(f"Found sheets collection: {collection_name!r} with {len(collection_parts)} parts")

        # build collection details
        collection_details = {
            'name': collection_name,
            'poster_url': collection_poster,
            'overview': collection_summary,
            'parts': []
        }
        for collection_part in collection_parts:
            # validate tmdb id is valid
            trimmed_tmdb_id = collection_part.strip()
            if not trimmed_tmdb_id.isalnum():
                logger.error(f"Collection {collection_name!r} had an invalid part: {trimmed_tmdb_id!r}")
                continue

            # lookup tmdb movie details
            movie_details = themoviedb.get_tmdb_id_details(trimmed_tmdb_id)
            if movie_details is not None:
                collection_details['parts'].append(movie_details)

        return collection_details

    except Exception:
        logger.exception(f"Exception retrieving collection from sheets with id {id!r}: ")
    return None


def get_all_sheets_collections(last_run_timestamp=None):
    logger.debug("Retrieving all collections from sheets")
    try:
        # open sheet
        collections = pd.read_csv(SHEETS_URL, index_col=0)

        # build list of all available collections
        collection_details = {}
        for id in range(1, len(collections)):
            item = collections.loc[int(id), :]
            collection_name = str(item[0])
            collection_poster = str(item[1])
            collection_summary = str(item[2])
            collection_parts = str(item[3]).split(',')
            collection_timestamp = str(item[4])

            # validate entry has all the required data
            if not collection_name or collection_name == 'nan' \
                    or not collection_poster or collection_poster == 'nan' \
                    or not collection_summary or collection_summary == 'nan' \
                    or 'nan' in collection_parts \
                    or not collection_timestamp or collection_timestamp == 'nan':
                logger.trace(f"Skipping sheets collection with id {id!r} as it did not have the required settings")
                continue

            # compare last_run_timestamp
            if last_run_timestamp and misc.is_utc_timestamp_before(last_run_timestamp, collection_timestamp):
                continue

            # add collection to list
            collection_details[str(id)] = {
                'name': collection_name,
                'poster_url': collection_poster,
                'overview': collection_summary,
                'timestamp': collection_timestamp,
                'parts': collection_parts
            }

        return collection_details

    except Exception:
        logger.exception("Exception retrieving all available collections from sheets: ")
    return None
