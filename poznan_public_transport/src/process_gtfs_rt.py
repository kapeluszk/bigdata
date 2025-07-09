from google.transit import gtfs_realtime_pb2
import os
from pyspark.sql import SparkSession
import io
from datetime import datetime, timedelta
import re

def parse_gtfs_rt(filepath):
    """
    Parse GTFS RT data from the given file path.
    """

    feed = gtfs_realtime_pb2.FeedMessage()
    with open(filepath, 'rb') as f:
        feed.ParseFromString(f.read())

    return feed

def find_latest_file(name, directory):
    """
    Find the latest file in the given directory with the specified name.
    """
    files = [f for f in os.listdir(directory) if f.startswith(name)]
    if not files:
        return None
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(directory, x)))
    return os.path.join(directory, latest_file)

def create_df(feed, spark):
    rows = []
    for entity in feed.entity:
        route_id_full = entity.trip_update.trip.route_id
        trip_id = entity.trip_update.trip.trip_id
        schedule_relationship = entity.trip_update.trip.schedule_relationship

        route_id_match = re.match(r'^(\d+)', route_id_full)
        route_id = route_id_match.group(1) if route_id_match else route_id_full

        for stop_update in entity.trip_update.stop_time_update:
            delay = None
            if stop_update.HasField('arrival'):
                delay = stop_update.arrival.delay

            rows.append({
                "route_id": route_id,
                "trip_id": trip_id,
                "schedule_relationship": schedule_relationship,
                "delay": delay,
            })

    return spark.createDataFrame(rows)

if __name__ == "__main__":
    filepath = "data/raw/gtfs_rt/"
    feed = parse_gtfs_rt(find_latest_file("trip_updates", filepath))

    # print(feed)


    spark = SparkSession.builder \
        .appName("Poznan ZTM") \
        .master("local[*]") \
        .getOrCreate()

    processed_df = create_df(feed, spark)
    processed_df.show()
