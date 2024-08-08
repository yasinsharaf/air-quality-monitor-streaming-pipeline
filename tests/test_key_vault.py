import sys
import os

#append src dir to current dir. This has to be executed before importing the utils
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..', 'src'))

import pytest
from utils.ingestion_utils import OpenWeatherAPIGeocoding, FetchSecret



#params
@pytest.fixture(scope="session")    #fetch key once and store in fixture to avoid consuming calls when testing
def api_key():
    return FetchSecret("OpenWeatherAPIKeyYasin")


def test_api_key(api_key, capfd):
    print("API Key:",api_key)
    assert api_key is not None  # make sure the key vault is returning something
    assert isinstance(api_key, str)  #make sure you are receiving a string

    #capture output aka api key
    captured = capfd.readouterr()
    print(captured.out)
    
    



