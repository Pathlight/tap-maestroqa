import requests
import singer

LOGGER = singer.get_logger()


class MaestroQAAPI:
    BASE_URL = 'https://app.maestroqa.com/api/v1'
    MAX_GET_ATTEMPTS = 10
    MAX_POST_ATTEMPTS = 10

    def __init__(self, config):
        self.api_key = config['api_key']
        self.headers = {'apiToken': config['api_key']}

    # POST request kicks off a raw export
    def post(self, url, params=None):
        if not url.startswith('http://'):
            url = f'{self.BASE_URL}/{url}'

        LOGGER.info(f'Maestro POST {url}')

        resp = requests.post(url, json=params, headers=self.headers)

        return resp.json()

    # GET request retrieves the results of the export
    def get(self, url, params=None):
        if not url.startswith('http://'):
            url = f'{self.BASE_URL}/{url}'

        LOGGER.info(f'Maestro GET {url}')

        resp = requests.get(url, json=params, headers=self.headers)

        return resp.json()
