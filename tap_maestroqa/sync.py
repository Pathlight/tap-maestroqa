import codecs  # to help with byte vs str issues in the csv download
import csv
import singer
import datetime
import requests
import time
from contextlib import closing
from .client import MaestroQAAPI

LOGGER = singer.get_logger()


def write_schema_from_header(entity, header, keys):
    schema = {
        "type": "object",
        "properties": {}
    }
    header_map = []
    for column in header:
        # for now everything is a string; ideas for later:
        # 1. intelligently detect data types based on a sampling of entries from the raw data
        # 2. by default everything is a string, but allow entries in config.json to hard-type columns by name
        schema["properties"][column] = {"type": "string"}
        header_map.append(column)

    singer.write_schema(
        stream_name=entity,
        schema=schema,
        key_properties=keys,
    )


def get_file(client, stream, state=None):

    # ISOFormat datestrings are of this date format:
    # [year]-[month]-[day]T[hour]:[minute]:[second]Z
    # ex. 2017-12-13T05:00:00Z

    # make a post request (start date from state, end date is current time)
    params = {
        'startDate': '2020-06-24T00:00:00Z',  # TODO: replace with date from state
        'endDate': '2020-06-25T00:00:00Z',  # TODO: replace with now as string?
        # 'name': '',
        'singleFileExport': stream,
    }

    export_id = client.post(params)

    # wait for the export to complete
    time.sleep(10)  # TODO: make this smarter
    export_results = client.get(export_id)

    # get the file
    return export_results['dataUrl']


def process_file(stream, file_url):
    LOGGER.info("Syncing CSV file")

    # read data from csv url (from https://stackoverflow.com/a/38677650)
    with closing(requests.get(file_url, stream=True)) as r:
        reader = csv.DictReader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=',', quotechar='"')
        keys = None
        if stream == 'total_scores':
            keys = 'gradable_id'
        else:
            # section_scores has no unique id per row
            # todo: possibly combine gradable id + section_id to create one
            pass
        write_schema_from_header(stream, reader.fieldnames, keys)
        bookmark = '2020-06-24T00:00:00Z'  # TODO: replace with state
        for row in reader:
            record = {}
            for key, value in row.items():
                record[key] = value
            if len(record) > 0:  # only write records for non-empty lines
                singer.write_record(stream, record)
                if row['date_graded'] > bookmark:
                    bookmark = row['date_graded']
                    singer.write_state({stream: bookmark})


def sync(config, state, catalog):
    '''Sync data from tap source'''
    client = MaestroQAAPI(config)

    # loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info(f'Syncing stream {stream.tap_stream_id}')
        if stream.tap_stream_id == 'total_scores':
            file_url = get_file(client, stream, state)
            process_file(stream, file_url)
