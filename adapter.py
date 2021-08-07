# see https://api.dclimate.net/ for reference
# implementing metadata and simple gridded data retrieval (for now)

import ipfshttpclient, json, datetime, os, tarfile, gzip, pickle, zipfile
from adapter_utils import get_gridcell_ipfs, get_metadata_http
from extra_tools import get_heads

GATEWAY_IPFS_ID = '/ip4/134.122.126.13/tcp/4001/p2p/12D3KooWM8nN6VbUka1NeuKnu9xcKC56D17ApAVRDyfYNytzUsqG'

class Adapter:

    def __init__(self, input, ipfs_timeout=None):

        self.id = input.get('id', '1')
        self.request_data = input.get('data')
        self.ipfs_timeout = ipfs_timeout

        self.task_map = {'dataset_information': 0, 'grid_file_dataset_history': 1}
        self.task_params = [
                            ['dataset'],#'full_metadata']
                            ['lat', 'lon', 'dataset', 'also_return_metadata', 'use_imperial_units', 'also_return_snapped_coordinates', 'convert_to_local_time', 'as_of'],
                            # ['station_id', 'weather_variable', 'dataset', 'use_imperial_units'],
        ]

        if self.validate_request_data():
            self.ipfs = ipfshttpclient.connect(timeout=ipfs_timeout)
            self.set_params()
            self.create_request()
        else:
            self.result_error('No data provided')

    def validate_request_data(self):
        if self.request_data is None:
            return False
        if self.request_data == {}:
            return False
        return True

    def set_params(self):
        try:
            task_name = self.request_data.get('task')
            self.params = {'task': self.task_map.get(task_name)}
            for param in self.task_params[self.params.get('task')]:
                self.params[param] = self.request_data.get(param)
        except AttributeError as e:
            raise e

    def get_metadata(self):
        hash_str = get_heads()[self.params.get('dataset')]
        result = get_metadata_http(hash_str)
        return result

    def get_gridded_data(self):
        result = get_gridcell_ipfs(
                                lat=self.params.get('lat'),
                                lon=self.params.get('lon'),
                                dataset=self.params.get('dataset'),
                                also_return_snapped_coordinates=self.params.get('also_return_snapped_coordinates', False),
                                also_return_metadata=self.params.get('also_return_metadata', False),
                                use_imperial_units=self.params.get('use_imperial_units', True),
                                convert_to_local_time=self.params.get('convert_to_local_time', True),
                                as_of=self.params.get('as_of', None),
                                ipfs_timeout=self.ipfs_timeout,
        )
        return result

    def trigger_ipfs_request(self):
        task_parsers = [self.get_metadata, self.get_gridded_data,]
        parser = task_parsers[self.params.get('task')]
        result = parser()
        return result

    def create_request(self):
        try:
            results = self.trigger_ipfs_request()
            data = results[0]
            if self.params.get('also_return_snapped_coordinates', False):
                data['snapped to'] = results[1]
            if self.params.get('also_return_metadata', False):
                data['metadata'] = results[2]
            self.result_success(data)
        except Exception as e:
            self.result_error(e)

    def result_success(self, data):
        self.result = {
            'jobRunID': self.id,
            'data': data,
            'statusCode': 200,
        }

    def result_error(self, error):
        self.result = {
            'jobRunID': self.id,
            'status': 'errored',
            'error': f'There was an error: {error}',
            'statusCode': 500,
        }
