import pytest
import adapter

job_run_id = '1'


def adapter_setup(test_data):
    a = adapter.Adapter(test_data)
    return a.result


@pytest.mark.parametrize('test_data', [
    {'id': job_run_id, 'data': {'task': 'dataset_information', 'dataset': 'cpcc_precip_us-daily'}},
    {'id': job_run_id, 'data': {'task': 'grid_file_dataset_history', 'dataset': 'cpcc_precip_us-daily', 'lat': 41.3, 'lon': -82.2}},
])
def test_create_request_success(test_data):
    result = adapter_setup(test_data)
    print(result)
    assert result['statusCode'] == 200
    assert result['jobRunID'] == job_run_id
    assert result['data'] is not None

@pytest.mark.parametrize('test_data', [
    {'id': job_run_id, 'data': {}},
])
def test_create_request_error(test_data):
    result = adapter_setup(test_data)
    print(result)
    assert result['statusCode'] == 500
    assert result['jobRunID'] == job_run_id
    assert result['status'] == 'errored'
    assert result['error'] is not None
