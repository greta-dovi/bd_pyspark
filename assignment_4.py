import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from geopy.distance import geodesic
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Data Preparation

# Load the data into a PySpark DataFrame.
spark = SparkSession.builder.appName("vesselsSQL").getOrCreate()

vessels = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("aisdk-2025-01-23.csv")


# Ensure that the data types for latitude, longitude, and timestamp are appropriate for calculations and sorting.
sub_vessels = vessels.select("MMSI", "# Timestamp", "Latitude", "Longitude", )

sub_vessels = sub_vessels.withColumn("date_time", F.to_timestamp("# Timestamp", "dd/MM/yyyy HH:mm:ss"))
sub_vessels = sub_vessels.filter(F.abs(sub_vessels.Latitude) < 90)
sub_vessels = sub_vessels.filter(F.abs(sub_vessels.Longitude) < 180)
sub_vessels.printSchema()


# Data Processing with PySpark
# Calculate the distance between consecutive positions for each vessel using a suitable geospatial library or custom function that can integrate with PySpark.
# Aggregate these distances by MMSI to get the total distance traveled by each vessel on that day.

# Example
def geodesic_dist(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).kilometers

geodesic_udf = F.udf(geodesic_dist, DoubleType())

w = Window.partitionBy("MMSI").orderBy("date_time")
sub_vessels = sub_vessels.withColumn("lag_latitude", F.lag(F.col("Latitude")).over(w))
sub_vessels = sub_vessels.withColumn("lag_longitude", F.lag(F.col("Longitude")).over(w))
sub_vessels = sub_vessels.filter(sub_vessels.lag_latitude.isNotNull() & 
                                 sub_vessels.lag_longitude.isNotNull())
sub_vessels = sub_vessels.withColumn("distance_kilometers", geodesic_udf("Latitude",
                                                                     "Longitude",
                                                                     "lag_latitude",
                                                                     "lag_longitude"))
sub_vessels.show()

# Identifying the Longest Route
# Sort or use an aggregation function to determine which vessel traveled the longest distance.

total_distance = sub_vessels.groupBy("MMSI").agg(F.round(F.sum("distance_kilometers"), 3).alias("total_distance_km"))
total_distance_sorted = total_distance.sort(F.col("total_distance_km").desc())
total_distance_sorted.show()





# Output
# The final output should be the MMSI of the vessel that traveled the longest distance, along with the computed distance.

# Code Documentation and Comments
# Ensure the code is well-documented, explaining key PySpark transformations and actions used in the process.

# Deliverables
# A PySpark script that completes the task from loading to calculating and outputting the longest route.
# A brief report or set of comments within the code that discusses the findings and any interesting insights about the data or the computation process.

spark.stop()
