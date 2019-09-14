from loguru import logger

from utils import sql


def find_library_type(database_path, library_name):
    query_str = """SELECT
                    ls.id, ls.name, ls.section_type
                    FROM library_sections ls
                    WHERE ls.name = ?
                    LIMIT 1"""
    # lookup info
    result = sql.get_query_result(database_path, query_str, [library_name])
    if result is None:
        logger.debug(f"Failed to find library with name: {library_name!r}")
        return None

    # check we have a valid result
    if 'id' in result and 'section_type' in result:
        logger.debug(f"Found library with name: {library_name!r} (ID: {result['id']} - Type: {result['section_type']})")
        return result['section_type']

    # we have an unexpected result?
    logger.debug(f"Failed to find library with name: {library_name!r}, unexpected query result:\n{result}")
    return None
