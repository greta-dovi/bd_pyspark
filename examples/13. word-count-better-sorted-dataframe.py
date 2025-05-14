import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("book.txt")

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())
