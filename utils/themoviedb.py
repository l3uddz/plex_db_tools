import json

import tmdbsimple as tmdb
from loguru import logger

from . import misc


def get_tmdb_collection_parts(tmdb_id):
    logger.debug(f"Retrieving movie collection details for: {tmdb_id!r}")
    try:
        collection_details = {}

        # set api key
        tmdb.API_KEY = 'da6bf4ac38be518f95bbb3c309fad7b9'

        # get collection details
        details = tmdb.Collections(tmdb_id).info()
        if not isinstance(details, dict) or not misc.dict_contains_keys(details,
                                                                        ['id', 'name', 'parts', 'poster_path']):
            logger.error(f"Failed retrieving movie collection details from Tmdb for: {tmdb_id!r}")
            return None
        logger.trace(f"Retrieved Tmdb movie collection details for {tmdb_id!r}:")
        logger.trace(json.dumps(details, indent=2))

        # build collection_details
        collection_details['name'] = details['name']
        collection_details['poster_url'] = f"https://image.tmdb.org/t/p/original{details['poster_path']}"
        collection_details['overview'] = details['overview'] if 'overview' in details else ''

        for collection_part in details['parts']:
            if not misc.dict_contains_keys(collection_part, ['id', 'title']):
                logger.error(f"Failed processing collection part due to unexpected keys: {collection_part}")
                return None

            # retrieve movie details
            logger.debug(f"Retrieving movie details for collection part: {collection_part['title']!r} - "
                         f"TmdbId: {collection_part['id']}")
            movie = tmdb.Movies(collection_part['id']).info()

            # validate response
            if not isinstance(movie, dict) or not misc.dict_contains_keys(movie, ['id', 'imdb_id', 'title']):
                logger.error(f"Failed retrieving movie details from Tmdb for: {collection_part['title']!r} - "
                             f"TmdbId: {collection_part['id']}")
                return None

            # add part to collection details
            if 'parts' in collection_details:
                collection_details['parts'].append({
                    'title': movie['title'],
                    'tmdb_id': movie['id'],
                    'imdb_id': movie['imdb_id']
                })
            else:
                collection_details['parts'] = [{
                    'title': movie['title'],
                    'tmdb_id': movie['id'],
                    'imdb_id': movie['imdb_id']
                }]

            logger.trace(f"Retrieved Tmdb movie details for {collection_part['id']!r}:")
            logger.trace(json.dumps(movie, indent=2))

        logger.debug(json.dumps(collection_details, indent=2))
        return collection_details

    except Exception:
        logger.exception(f"Exception retrieving Tmdb collection details for {tmdb_id!r}: ")
    return None
