import requests, zipfile
from argparse import ArgumentParser
from pathlib import Path
from requests.exceptions import HTTPError
import io
from concurrent.futures import ThreadPoolExecutor
import shutil, os


download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]

download_directory = Path(__file__).parent / "downloads"

parser = ArgumentParser()
parser.add_argument("--call_async", action="store_true")

def download_and_unzip(uri, download_directory):
    with requests.Session() as s:
        with s.get(uri, stream=True) as response:
            try:
                response.raise_for_status()
                zipfile.ZipFile(io.BytesIO(response.content)).extractall(
                    download_directory
                )
            except HTTPError as h:
                print(f"Invalid request for {uri}")
                print(h)

def create_directory(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)
        
def main(download_dir, uris):
    create_directory(download_dir)
    
    if parser.parse_args().call_async:
        with ThreadPoolExecutor() as pool:
            _ = [pool.submit(download_and_unzip, uri, download_directory) for uri in uris]
    else:
        for uri in uris:
            download_and_unzip(uri, download_dir)

    mac_folder = download_dir / '__MACOSX'
    if os.path.exists(mac_folder):
        shutil.rmtree(mac_folder)

if __name__ == '__main__':
    main(download_directory, download_uris)
