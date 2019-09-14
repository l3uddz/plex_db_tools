from loguru import logger

from utils import sql
from . import library

METADATA_MISSING_QUERY_STRINGS = {
    '1': """SELECT
            ls.name as library_name
            , md.*
            FROM metadata_items md
            JOIN library_sections ls ON ls.id = md.library_section_id
            WHERE 
            ls.name = ?
            AND md.metadata_type = 1
            AND (md.user_thumb_url like 'media://%' OR md.user_thumb_url = '')
            ORDER BY md.added_at ASC""",
    '2': """SELECT
            ls.name as library_name
            , md.*
            FROM metadata_items md
            JOIN library_sections ls ON ls.id = md.library_section_id
            WHERE 
            ls.name = ?
            AND md.metadata_type = 2
            AND md.user_thumb_url = ''
            ORDER BY md.added_at ASC"""
}


def find_items_missing_posters(database_path, library_name):
    logger.debug(f"Finding items with missing posters from library: {library_name!r}")

    # determine library type
    library_type = library.find_library_type(database_path, library_name)
    if library_type is None:
        logger.error(f"Failed to find library type for library: {library_name!r}")
        return None

    # do we have a query for this library type
    if str(library_type) not in METADATA_MISSING_QUERY_STRINGS:
        logger.error(f"Unable to find items with missing posters for this library type: {library_type!r}")
        return None

    # find items
    query_str = METADATA_MISSING_QUERY_STRINGS[str(library_type)]
    return sql.get_query_results(database_path, query_str, [library_name])
