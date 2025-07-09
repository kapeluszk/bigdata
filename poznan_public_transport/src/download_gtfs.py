import os
import requests
import zipfile

GTFS_URL = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile'
GTFS_FILENAME = 'gtfs.zip'

def download_gtfs(filename, url, save_dir="data/raw/gtfs"):
    """
    Download GTFS data from the given URL and save it to the specified directory.
    """
    os.makedirs(save_dir, exist_ok=True)
    filepath = os.path.join(save_dir, filename)

    response = requests.get(url)
    if response.status_code == 200:
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"Downloaded GTFS data to {filepath}")
    else:
        print(f"Failed to download GTFS data: {response.status_code}")

    # Unzip the downloaded file
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(save_dir)
    print(f"Extracted GTFS data to {save_dir}")
    os.remove(filepath)

if __name__ == "__main__":
    download_gtfs(GTFS_FILENAME, GTFS_URL)