import pytest
from main import main
import os

@pytest.fixture(scope="class")
def uris():
    download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    ]
    return download_uris 

@pytest.fixture(scope="session")
def download_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("downloads_tmp")


class TestExercise1:
    
    @pytest.fixture(scope="class", autouse=True)
    def download_act(self, download_dir, uris):
        download_directory = download_dir
        download_uris = uris
        main(download_directory, download_uris)
    
    def test_is_all_csvs(self, download_dir):
        files = download_dir.glob("*")
        for file in files:
            assert str(file).endswith("csv")
    
    def test_not_exsits_macfolder(self, download_dir):
        dirs = download_dir.glob("*"+ os.path.sep)
        for dir in dirs:
            assert "__MACOSX" not in str(dir)