#!/usr/bin/env python3
import logging
import os
import sys

import click
import requests
from loguru import logger
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tabulate import tabulate

import plex
from utils import misc

############################################################
# INIT
############################################################


# Globals
cfg = None
manager = None

# Logging
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# Click
@click.group(help='plex_db_tools')
@click.version_option('1.0.0', prog_name='plex_db_tools')
@click.option(
    '-v', '--verbose',
    envvar="LOG_LEVEL",
    count=True,
    default=0,
    help='Adjust the logging level')
@click.option(
    '--config-path',
    envvar='CONFIG_PATH',
    type=click.Path(file_okay=True, dir_okay=False),
    help='Configuration filepath',
    show_default=True,
    default=os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "config.json")
)
@click.option(
    '--log-path',
    envvar='LOG_PATH',
    type=click.Path(file_okay=True, dir_okay=False),
    help='Log filepath',
    show_default=True,
    default=os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "activity.log")
)
def app(verbose, config_path, log_path):
    global cfg

    # Ensure paths are full paths
    if not config_path.startswith(os.path.sep):
        config_path = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), config_path)
    if not log_path.startswith(os.path.sep):
        log_path = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), log_path)

    # Load config
    from utils.config import Config
    cfg = Config(config_path=config_path).cfg

    # Load logger
    log_levels = {0: 'INFO', 1: 'DEBUG', 2: 'TRACE'}
    log_level = log_levels[verbose] if verbose in log_levels else 'TRACE'
    config_logger = {
        'handlers': [
            {'sink': sys.stdout, 'backtrace': True if verbose >= 2 else False, 'level': log_level},
            {'sink': log_path,
             'rotation': '30 days',
             'retention': '7 days',
             'enqueue': True,
             'backtrace': True if verbose >= 2 else False,
             'level': log_level}
        ]
    }
    logger.configure(**config_logger)

    # Display params
    logger.info("%s = %r" % ("CONFIG_PATH".ljust(12), config_path))
    logger.info("%s = %r" % ("LOG_PATH".ljust(12), log_path))
    logger.info("%s = %r" % ("LOG_LEVEL".ljust(12), log_level))
    return


############################################################
# COMMANDS
############################################################

@app.command(help='Find unalayzed media items')
@click.option(
    '-l', '--library',
    help='Library to search for unanalyzed items', required=True
)
@click.option('--auto-mode', '-a', required=False, default='0', help='Automatically perform specific action')
def unanalyzed_media(library, auto_mode):
    global cfg

    # retrieve items with unanalyzed media
    results = plex.metadata.find_items_unanalyzed(cfg.plex.database_path, library)
    if results is None:
        logger.error(f"Failed to find unanalyzed media items for library: {library!r}")
        sys.exit(1)

    if not results:
        logger.info(f"There were no media items without analysis in library: {library!r}")
        sys.exit(0)

    logger.info(f"Found {len(results)} media items without analysis in library: {library!r}")

    # process found items
    for item in results:
        if 'file' not in item or 'metadata_item_id' not in item:
            logger.debug(f"Skipping item as there was no title or metadata_item_id found: {item}")
            continue

        logger.info(f"Media analysis was required for: {item['file']}")

        if auto_mode == '0':
            # ask user what to-do
            logger.info("What would you like to-do with this item? (0 = skip, 1 = analyze)")
            user_input = input()
            if user_input is None or user_input == '0':
                continue
        else:
            # user the determined auto mode
            user_input = auto_mode

        # act on user input
        if user_input == '1':
            # do analyze
            logger.debug("Analyzing metadata...")
            if plex.actions.analyze_metadata_item(cfg, item['metadata_item_id']):
                logger.info("Media analysis successful!")
            else:
                continue

    logger.info("Finished")
    sys.exit(0)


@app.command(help='Find missing posters')
@click.option(
    '-l', '--library',
    help='Library to search for missing posters', required=True)
@click.option('--auto-mode', '-a', required=False, default='0', help='Automatically perform specific action')
def missing_posters(library, auto_mode):
    global cfg

    # retrieve items with missing posters
    results = plex.metadata.find_items_missing_posters(cfg.plex.database_path, library)
    if results is None:
        logger.error(f"Failed to find missing posters for library: {library!r}")
        sys.exit(1)

    if not results:
        logger.info(f"There were no items with missing posters in library: {library!r}")
        sys.exit(0)

    logger.info(f"Found {len(results)} items with missing posters in the library: {library!r}")

    # process found items
    for item in results:
        if 'title' not in item or 'id' not in item:
            logger.debug(f"Skipping item as there was no title or metadata_item_id found: {item}")
            continue

        # build table data for this item
        table_data = [
            # Library
            ['Library', item['library_name'] if misc.valid_dict_item(item, 'library_name') else '']
            # Metadata Item ID
            , ['ID', item['id']]
            # GUID
            , ['GUID', item['guid'] if misc.valid_dict_item(item, 'guid') else '']
            # Poster
            , ['Poster', item['user_thumb_url'] if misc.valid_dict_item(item, 'user_thumb_url') else '']
            # Added date
            , ['Added', item['added_at'] if misc.valid_dict_item(item, 'added_at') else '']
        ]

        # show user information
        logger.info(f"Item with missing poster, {item['title']} "
                    f"({item['year'] if misc.valid_dict_item(item, 'year') else '????'}):"
                    f"\n{tabulate(table_data)}")

        if auto_mode == '0':
            # ask user what to-do
            logger.info("What would you like to-do with this item? (0 = skip, 1 = refresh)")
            user_input = input()
            if user_input is None or user_input == '0':
                continue
        else:
            # user the determined auto mode
            user_input = auto_mode

        # act on user input
        if user_input == '1':
            # do refresh
            logger.debug("Refreshing metadata...")
            if plex.actions.refresh_item_metadata(cfg, item['id']):
                logger.info("Refreshed metadata!")
            else:
                continue

    logger.info("Finished")
    sys.exit(0)


############################################################
# MAIN
############################################################

if __name__ == "__main__":
    app()
