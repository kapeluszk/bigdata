import os
import requests
from datetime import datetime

GTFS_RT_TRIP_UPDATES_URL = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file=trip_updates.pb'
GTFS_RT_VEHICLE_POSITIONS_URL = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file=vehicle_positions.pb'

def download_gtfs_rt(filename, url, save_dir="data/raw/gtfs_rt"):
    os.makedirs(save_dir, exist_ok=True)
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    filename += f"_{now}.pb"
    filepath = os.path.join(save_dir, filename)

    response = requests.get(url)
    if response.status_code == 200:
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"Downloaded trip updates to {filepath}")
    else:
        print(f"Failed to download trip updates: {response.status_code}")

if __name__ == "__main__":
    download_gtfs_rt("trip_updates", GTFS_RT_TRIP_UPDATES_URL)
    download_gtfs_rt("vehicle_positions", GTFS_RT_VEHICLE_POSITIONS_URL)
