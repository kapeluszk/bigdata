import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, desc, col, avg, current_timestamp
from process_gtfs_rt import find_latest_file, create_df, parse_gtfs_rt

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

    result = result.withColumn(
        "timestamp",
        current_timestamp()
    )

    return result


def save_to_iceberg(df, table_name):
    """
    Save DataFrame to an Iceberg table.
    :param df: DataFrame to save
    :param table_name: Name of the Iceberg table
    """
    if spark.catalog.tableExists(table_name):
        df.write.format("iceberg") \
            .mode("append") \
            .option("write-distribution-mode", "hash") \
            .partitionBy("timestamp") \
            .save(table_name)
    else:
        df.write.format("iceberg") \
            .mode("overwrite") \
            .option("write-distribution-mode", "hash") \
            .partitionBy("timestamp") \
            .save(table_name)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Iceberg Example") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.2.0") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "data/processed") \
        .getOrCreate()
    gtfs_data = process_gtfs(spark)
    gtfs_data.show()
    save_to_iceberg(gtfs_data, "spark_catalog.default.gtfs_data")