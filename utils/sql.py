import sqlite3
from contextlib import closing

from loguru import logger


def get_query_results(database_path, query_str, query_args):
    logger.trace(f"Running query {query_str!r} with args: {query_args}")
    try:
        with sqlite3.connect(database_path) as conn:
            conn.row_factory = sqlite3.Row
            with closing(conn.cursor()) as c:
                query_results = c.execute(query_str, query_args).fetchall()
                if not query_results:
                    logger.debug(f"No results were found from query")
                    return []

                results = [dict(result) for result in query_results]
                logger.debug(f"Found {len(results)} results from query")
                logger.trace(results)
                return results
    except Exception:
        logger.exception(f"Exception running query {query_str!r}: ")
    return None


def get_query_result(database_path, query_str, query_args):
    logger.trace(f"Running query {query_str!r} with args: {query_args}")
    try:
        with sqlite3.connect(database_path) as conn:
            conn.row_factory = sqlite3.Row
            with closing(conn.cursor()) as c:
                query_result = c.execute(query_str, query_args).fetchone()
                if not query_result:
                    logger.debug(f"No result was found from query")
                    return {}

                result = dict(query_result)
                logger.trace(f"Found result: {result}")
                return result
    except Exception:
        logger.exception(f"Exception running query {query_str!r}: ")
    return None
