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


def find_items_unanalyzed(database_path, library_name):
    logger.debug(f"Finding items without analysis from library: {library_name!r}")

    # build query_str
    query_str = """select
                    ls.name as library_name
                    , mp.file
                    , mi.*
                    from media_items mi
                    join media_parts mp on mp.media_item_id = mi.id
                    join library_sections ls on ls.id = mi.library_section_id
                    where mi.bitrate is null and ls.section_type in (1, 2) and ls.name = ?"""

    # retrieve results
    return sql.get_query_results(database_path, query_str, [library_name])


def get_metadata_item_id(database_path, metadata_item_id):
    logger.debug(f"Finding metadata_item details for id: {metadata_item_id!r}")

    # build query_str
    query_str = """SELECT
                    mi.id
                    , mi.library_section_id
                    , mi.metadata_type
                    , mi.guid
                    FROM metadata_items mi
                    WHERE mi.id = ?"""

    # retrieve result
    return sql.get_query_result(database_path, query_str, [metadata_item_id])


def get_metadata_item_by_guid(database_path, library_name, guid):
    logger.debug(f"Finding metadata_item details from library {library_name!r} with guid: {guid!r}")

    # build query_str
    query_str = """SELECT
                    ls.name
                    , mi.id
                    , mi.guid
                    , mi.title
                    , mi.year
                    FROM metadata_items mi
                    JOIN library_sections ls ON ls.id = mi.library_section_id
                    WHERE 
                    ls.name = ?
                    AND
                    mi.guid = ?"""

    # retrieve result
    return sql.get_query_result(database_path, query_str, [library_name, guid])
