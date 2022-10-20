from pathlib import Path


def create_directory(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)