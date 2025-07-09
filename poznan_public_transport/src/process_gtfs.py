import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, desc, col, avg
from process_gtfs_rt import find_latest_file, create_df, parse_gtfs_rt
from delta.tables import *

def process_gtfs(spark: SparkSession):

    gtfs_dir = "data/raw/gtfs"
    routes = spark.read.csv(os.path.join(gtfs_dir, "routes.txt"), header=True, inferSchema=True)
    trips = spark.read.csv(os.path.join(gtfs_dir, "trips.txt"), header=True, inferSchema=True)
    agency = spark.read.csv(os.path.join(gtfs_dir, "agency.txt"), header=True)

    curr_delay = find_latest_file("trip_updates","data/raw/gtfs_rt/")
    feed = parse_gtfs_rt(curr_delay)
    feed_df = create_df(feed,spark)

    common_trips = trips.select("trip_id").intersect(feed_df.select("trip_id"))
    print("Liczba wspólnych trip_id:", common_trips.count())


    result = routes.join(trips, "route_id") \
            .join(agency, "agency_id") \
            .where(col("agency_id") == 2) \
            .join(feed_df, "route_id") \
            .select("route_short_name", "delay") \
            .groupBy("route_short_name") \
            .agg(avg("delay").alias("avg_delay")) \
            .orderBy("avg_delay", ascending=False)

    result = result.withColumn(
        "vehicle_type",
        when(col("route_short_name").cast("int") < 100, "tram").otherwise("bus")
    )

    return result

def save_to_delta(df, path="data/processed/gtfs"):
    """
    Save the DataFrame to Delta format.
    """
    delta_table = delta.tables.DeltaTable.forPath(spark, path)
    if delta_table.isEmpty():
        df.write.format("delta").mode("overwrite").save(path)
    else:
        df.write.format("delta").mode("append").save(path)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Poznan ZTM") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    gtfs_data = process_gtfs(spark)
    gtfs_data.show()
    save_to_delta(gtfs_data, "data/processed/gtfs")