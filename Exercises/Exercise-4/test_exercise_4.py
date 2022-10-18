import pytest
import json
from tempfile import NamedTemporaryFile

@pytest.fixture()
def input_json():
    return json.loads('{"type":"Point","coordinates":[-99.9,16.88333]}')

@pytest.fixture()
def flatten_res():
    return {
        'type': 'Point',
        'coordinates_0': -99.9,
        'coordinates_1': 16.88333
    }
    
def test_json_flatten(input_json, flatten_res):
    from main import json_flatten
    with NamedTemporaryFile(delete=True, mode='w+') as tmp:
        json.dump(input_json, tmp)
        tmp.flush()
        res = json_flatten(tmp.name)
    assert res == flatten_res