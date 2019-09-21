import pandas as pd
from loguru import logger

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
        collection_name = item[0]
        collection_poster = item[1]
        collection_summary = item[2]
        collection_parts = item[3].split(',')
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
