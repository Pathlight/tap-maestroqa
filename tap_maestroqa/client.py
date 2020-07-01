import requests
import singer
import time


LOGGER = singer.get_logger()


class MaestroAPI:
    BASE_URL = 'https://app.maestroqa.com/api/v1'
    MAX_GET_ATTEMPTS = 10
    MAX_POST_ATTEMPTS = 10

    def __init__(self, config):
        self.headers = {'apiToken': config['api_token']}

    def post(self, params):
        '''Kicks off a raw export, returns an export ID'''

        url = f'{self.BASE_URL}/request-raw-export'

        LOGGER.info(f'MaestroQA POST request to start an export {url}')
        LOGGER.info(f'params: {params}')

        resp = requests.post(url, json=params, headers=self.headers)

        return resp.json()

    def get(self, params):
        '''Given an export ID, returns a result status and, when the
        export is complete, a URL for CSV download'''

        url = f'{self.BASE_URL}/get-export-data'

        LOGGER.info(f'MaestroQA GET request to retrieve the export {url}')

        for num_retries in range(self.MAX_GET_ATTEMPTS):
            will_retry = num_retries < self.MAX_GET_ATTEMPTS - 1

            # TO DO: figure out if/when/what errors we received, try & except
            resp = requests.get(url, json=params, headers=self.headers)

            result = resp.json()

            if result['status'] == 'complete':
                break

            elif result['status'] in ['requested', 'in_progress'] and will_retry:
                time.sleep(10)

        resp.raise_for_status()
        return resp.json()
