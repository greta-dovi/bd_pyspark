import os
import sys
from pyspark.sql import SparkSession
# from pyspark.sql import Row
from pyspark.sql import functions as func

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

# Select only age and numFriends columns
friendsByAge = lines.select("age", "friends")

# From friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# Sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# Formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()

spark.stop()
