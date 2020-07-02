import codecs  # to help with byte vs str issues in the csv download
import csv
import singer
import datetime
import pytz

import requests
import time
from contextlib import closing
from singer.utils import strptime_to_utc, strftime as singer_strftime
from .client import MaestroAPI

LOGGER = singer.get_logger()
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def transform_date(datestr):
    if datestr.startswith('='):
        date_obj = datetime.datetime.strptime(datestr, '="%Y-%m-%d %H:%M:%S.%f"').replace(tzinfo=pytz.UTC)
    else:
        date_obj = strptime_to_utc(datestr)
    # reformat to use RFC3339 format
    value = singer_strftime(date_obj)
    return value


def get_file(client, stream, state=None):

    # UNCOMMENT WHEN DONE TESTING
    end_date = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    params = {
        'startDate': state['bookmarks'][stream.tap_stream_id]['date_graded'],
        'endDate': end_date,  # TODO: replace with now as string?
        'name': f'{stream.tap_stream_id} thru {end_date}',
        'singleFileExport': stream.tap_stream_id,
    }

    # UNCOMMENT WHEN DONE TESTING
    export_id = client.post(params)
    LOGGER.info(f'{export_id}')
    LOGGER.info("Successfully made get request")
    # export_id = {'exportId': 'id_z3A4qrYyYqcC5vMQ9'}

    # wait for the export to complete
    time.sleep(5)  # TODO: make this smarter
    export_results = client.get(export_id)

    # get the file
    return export_results['dataUrl']


def process_file(stream, state, file_url):
    LOGGER.info("Syncing CSV file")

    # read data from csv url using a generator (from https://stackoverflow.com/a/38677650)
    with closing(requests.get(file_url, stream=True)) as r:
        reader = csv.DictReader(
            codecs.iterdecode(r.iter_lines(), 'utf-8'),
            delimiter=',',
            quotechar='"')
        LOGGER.info('Created csv dictreader')

        # the csv export doesn't produce a sorted file, so sort here in order
        # to update state in chronological order in case of interruption
        reader = sorted(reader, key=lambda d: d['date_graded'])

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        date_fields = set(['date_graded', 'ticket_created_at', 'date_first_started', 'date_first_graded'])
        integer_fields = set(['max_section_score', 'section_score', 'max_rubric_score'])
        float_fields = set(['rubric_score'])
        bookmark = state['bookmarks'][stream.tap_stream_id]['date_graded']
        for row in reader:
            record = {}
            for key, value in row.items():
                if key in date_fields:
                    value = transform_date(value)
                elif key in integer_fields and value:
                    value = int(value)
                elif key in float_fields:
                    value = float(value)
                elif not value:
                    value = None
                record[key] = value
            if len(record) > 0:  # only write records for non-empty lines
                singer.write_record(stream.tap_stream_id, record)
                new_bookmark = transform_date(row['date_graded'])
                if new_bookmark > bookmark:
                    bookmark = row['date_graded']
                    singer.write_state({state['bookmarks'][stream.tap_stream_id]['date_graded']: new_bookmark})


def sync(config, state, catalog):
    '''Sync data from tap source'''
    client = MaestroAPI(config)

    # loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info(f'Syncing stream {stream.tap_stream_id}')
        file_url = get_file(client, stream, state)
        LOGGER.info('Retrieved')
        process_file(stream, state, file_url)
