import requests
import requests.exceptions
import singer
import time
# from fuji.queries.exceptions import APIQueryError

LOGGER = singer.get_logger()


class MaestroQaAPI:
    BASE_URL = 'https://app.maestroqa.com/api/v1'
    MAX_POST_ATTEMPTS = 7
    MAX_GET_ATTEMPTS = 10

    def __init__(self, config):
        self.headers = {'apiToken': config['api_token']}

    def post(self, params):
        '''Kicks off a raw export, returns an export ID'''

        url = f'{self.BASE_URL}/request-raw-export'

        for num_retries in range(self.MAX_POST_ATTEMPTS):
            LOGGER.info(f'MaestroQA POST request to start an export {url}')
            will_retry = num_retries < self.MAX_POST_ATTEMPTS - 1
            try:
                resp = requests.post(url, json=params, headers=self.headers)
            except requests.exceptions.RequestException:
                resp = None
                if will_retry:
                    LOGGER.info('MaestroQA: unable to get response, will retry', exc_info=True)
                else:
                    LOGGER.info(f'MaestroQA: unable to get response, exceeded retries', exc_info=True)

            if will_retry:
                if resp and resp.status_code >= 500:
                    LOGGER.info('MaestroQA request with 5xx response, retrying', extra={
                        'url': resp.url,
                        'reason': resp.reason,
                        'code': resp.status_code
                    })
                elif resp and resp.status_code == 200:
                    break  # No retry needed
                time.sleep(60)

        # resp.raise_for_status()
        return resp.json()

    def get(self, params):
        '''Given an export ID, returns a result status and, when the
        export is complete, a URL for CSV download'''

        url = f'{self.BASE_URL}/get-export-data'

        LOGGER.info(f'MaestroQA GET request to retrieve the export {url}')

        for num_retries in range(self.MAX_GET_ATTEMPTS):
            will_retry = num_retries < self.MAX_GET_ATTEMPTS - 1

            try:
                resp = requests.get(url, json=params, headers=self.headers)
            except requests.exceptions.RequestException:
                resp = None
                if will_retry:
                    LOGGER.info('MaestroQA: unable to get response, will retry', exc_info=True)
                else:
                    LOGGER.info(f'MaestroQA: unable to get response, exceeded retries', exc_info=True)
                    break

            result = resp.json()

            if resp and resp.status_code >= 500:
                if will_retry:
                    LOGGER.info('MaestroQA request with 5xx response, retrying', extra={
                        'url': resp.url,
                        'reason': resp.reason,
                        'code': resp.status_code
                    })
                else:
                    LOGGER.info('MaestroQA request with 5xx response, exceeded retries', extra={
                        'url': resp.url,
                        'reason': resp.reason,
                        'code': resp.status_code
                    })
                    break
            # check to see if the export has finished successfully
            elif resp and resp.status_code == 200:
                if result['status'] == 'errored':
                    LOGGER.info('MaestroQA: export errored')
                    break  # no retry needed
                elif result['status'] == 'completed':
                    break  # no retry needed
            time.sleep(60)

        # resp.raise_for_status()
        return result
