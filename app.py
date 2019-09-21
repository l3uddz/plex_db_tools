#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
from datetime import datetime

import click
import requests
from loguru import logger
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tabulate import tabulate

import plex
from utils import misc, themoviedb, sheets

############################################################
# INIT
############################################################


# Globals
cfg = None
manager = None
runtime_settings_path = None

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
@click.option(
    '--rs-path',
    envvar='RS_PATH',
    type=click.Path(file_okay=True, dir_okay=False),
    help='Runtime settings filepath',
    show_default=True,
    default=os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "runtime_settings.json")
)
def app(verbose, config_path, log_path, rs_path):
    global cfg, runtime_settings_path

    # Ensure paths are full paths
    if not config_path.startswith(os.path.sep):
        config_path = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), config_path)
    if not log_path.startswith(os.path.sep):
        log_path = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), log_path)
    if not rs_path.startswith(os.path.sep):
        rs_path = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), rs_path)

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

    # Set other variables
    runtime_settings_path = rs_path

    # Display params
    logger.info("%s = %r" % ("CONFIG_PATH".ljust(12), config_path))
    logger.info("%s = %r" % ("RS_PATH".ljust(12), rs_path))
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


@app.command(help='Create or updat eall sheets movie collections')
@click.option(
    '-l', '--library',
    help='Library to search for missing posters', required=True)
@click.option(
    '-c', '--changes',
    help='Only process items that have changed since last run', is_flag=True
)
@click.option(
    '-i', '--ignore-missing',
    help='Ignore missing when generating last_run_changes', is_flag=True
)
def create_update_all_sheets_collections(library, changes, ignore_missing):
    global runtime_settings_path
    runtime_settings = {
        library: {
            'last_run_changes': {}
        }
    }

    if changes:
        runtime_settings = misc.load_settings(runtime_settings_path)
        if not runtime_settings or library not in runtime_settings or 'last_run_changes' not in runtime_settings[
            library]:
            logger.error(f"You must have run a full sync before changes mode will work!")
            sys.exit(1)

        logger.info(
            f"Retrieving all collections from Sheets with changes since last run")
    else:
        logger.info("Retrieving all available collections from Sheets...")

    # retrieve available collections
    collections = sheets.get_all_sheets_collections(
        None if library not in runtime_settings or 'last_run_changes' not in runtime_settings[library] else
        runtime_settings[library]['last_run_changes'])
    if not collections and not changes:
        logger.error("Failed to retrieve available collections from Sheets....")
        sys.exit(1)
    elif not collections and changes:
        logger.info(f"No collections were found with changes since last run")
        sys.exit(0)

    logger.info(f"Retrieved {len(collections)} collections from Sheets!")

    # process collections
    for id, collection in collections.items():
        logger.info(f"Processing collection with id {id}: {collection['name']!r}...")
        collection_result = create_update_collection.callback(library, tmdb_id=None, sheets_id=id)
        if collection_result is True or (ignore_missing and collection_result == -1):
            # update last runtime for this item
            runtime_settings[library]['last_run_changes'][str(id)] = datetime.utcnow().isoformat()

        time.sleep(2.5)

    # dump runtime_settings
    misc.dump_settings(runtime_settings_path, runtime_settings)

    # finish
    logger.info("Finished")
    sys.exit(0)


@app.command(help='Create or update movie collection')
@click.option(
    '-l', '--library',
    help='Library to search for missing posters', required=True)
@click.option(
    '-i', '--tmdb-id',
    help='The Movie Database Collection ID', required=False
)
@click.option(
    '-s', '--sheets-id',
    help='Cloudbox Sheets Collection ID', required=False
)
def create_update_collection(library, tmdb_id, sheets_id):
    overall_result = True

    if not tmdb_id and not sheets_id:
        logger.error("You must specify either a Tmdb ID or a Sheets ID!")
        return False

    if tmdb_id:
        logger.info(f"Retrieving details for Tmdb collection: {tmdb_id!r}")

        # retrieve collection details
        collection_details = themoviedb.get_tmdb_collection_parts(tmdb_id)
        if not collection_details or not misc.dict_contains_keys(collection_details, ['name', 'poster_url', 'parts']):
            logger.error(f"Failed retrieving details of Tmdb collection: {tmdb_id!r}")
            return False

        logger.info(
            f"Retrieved collection details: {collection_details['name']!r}, {len(collection_details['parts'])} parts")
    else:
        logger.info(f"Retrieving details for Sheets collection: {sheets_id!r}")
        collection_details = sheets.get_sheets_collection(sheets_id)

    # iterate collection items assigning them to the collection
    for item in collection_details['parts']:
        # try to find item by imdb guid
        plex_item_details = plex.metadata.get_metadata_item_by_guid(cfg.plex.database_path, library,
                                                                    f"com.plexapp.agents.imdb://{item['imdb_id']}"
                                                                    f"?lang=en")
        if not plex_item_details:
            # fallback to tmdb guid
            plex_item_details = plex.metadata.get_metadata_item_by_guid(cfg.plex.database_path, library,
                                                                        f"com.plexapp.agents.themoviedb://"
                                                                        f"{item['tmdb_id']}?lang=en")

        if not plex_item_details or not misc.dict_contains_keys(plex_item_details, ['id', 'guid', 'title', 'year']):
            logger.warning(
                f"Failed to find collection item in library: {library!r} - {item['title']}: {json.dumps(item)}")
            overall_result = -1
            continue

        # we have a plex item, lets assign it to the category
        logger.debug(
            f"Adding {plex_item_details['title']} ({plex_item_details['year']}) to collection: "
            f"{collection_details['name']!r}")

        if plex.actions.set_metadata_item_collection(cfg, plex_item_details['id'], collection_details['name']):
            logger.info(
                f"Added {plex_item_details['title']} ({plex_item_details['year']}) to collection: "
                f"{collection_details['name']!r}")
            time.sleep(2)
        else:
            logger.error(
                f"Failed adding {plex_item_details['title']} ({plex_item_details['year']}) to collection: "
                f"{collection_details['name']!r}")
            return False

    # locate collection in database
    logger.info("Sleeping 10 seconds before attempting to locate the collection in database")
    time.sleep(10)

    # lookup collection metadata_item_id
    collection_metadata = plex.metadata.get_metadata_item_of_collection(cfg.plex.database_path, library,
                                                                        collection_details['name'])
    if not collection_metadata or not misc.dict_contains_keys(collection_metadata, ['id', 'guid']):
        logger.error(
            f"Failed to find collection in the Plex library {library!r} with name: {collection_details['name']!r}")
        return False

    # set poster
    if collection_details['poster_url']:
        if not plex.actions.set_metadata_item_poster(cfg, collection_metadata['id'], collection_details['poster_url']):
            logger.error(f"Failed setting collection poster to: {collection_details['poster_url']!r}")
            return False

        logger.info(f"Updated collection poster to: {collection_details['poster_url']!r}")

    # set overview
    if collection_details['overview']:
        logger.info("Sleeping 5 seconds before setting collection summary")
        time.sleep(5)
        if not plex.actions.set_metadata_item_summary(cfg, collection_metadata['id'], collection_details['overview']):
            logger.error(f"Failed setting collection summary to: {collection_details['overview']!r}")
            return False

        logger.info(f"Updated collection summary to: {collection_details['overview']!r}")

    logger.info("Finished!")
    return overall_result


############################################################
# MAIN
############################################################

if __name__ == "__main__":
    app()
