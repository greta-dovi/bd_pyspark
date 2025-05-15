# Assignment 4
## Longest Route Traveled by a Vessel Using PySpark

##### Data Retrieval
For this project "aisdk-2025-01-23.csv" was chosen. 

##### Data Preparation
Data was loaded into PySpark DataFrame using `SparkSession`. The schema was inferred. <br>
Subset of the data was selected to reduce the requirements for memory. The variables necessary for the analysis were:
- "MMSI" - integer
- "# Timestamp" - string
- "Latitude" - double
- "Longitude" - double
Timestamp is converted to appropriate date-time format using Pyspark function `to_datetime`. Longitude and latitude are filtered to match the appropriate value range. <br>

##### Data Processing with PySpark
To calculate the distance between two consecutive coordinates, `geodesic` function from geopy library was used. This function takes in longitudes and latitudes and outputs the distance in chosen units (here - kilometers). However, for this function to be compatible with Pyspark, it was included in UDF (user defined function). UDFs enable users to create their own functions and perform complex processing tasks on Spark data frames. <br>
Additionally, to extract the consecutive coordinates from Spark dataframe, a lag function was applied over a window, which was grouped by unique MMSIs and sorted by date-time. 

##### Identifying the Longest Route
Total travelled distance was calculated by grouping values based on their MMSI and summing the distance differences. The final results were sorted in descending order. <br>
The vessels that travelled the longest distance are given in the table below. Additional information about the vessel type was taken from marinetraffic.com. 

| MMSI      | Km     | Vessel type | Comments |
|-----------|--------|:-----------:|----------|
| 2579999   | 67 136 |             |          |
| 211866190 | 15 325 |             |          |
| 111219516 | 13 889 |             |          |
| 245286000 | 9348   |             |          |
| 219005867 | 6568   |             |          |

##### Limitations
After inspecting the coordinates of the vessels that travelled the longest distance it was noticed that there are some inconsistencies with the locations. Some random location jumps appear (like one coordinate in Africa or similar), however, when trying to filter out the suspicious distances using `sub_vessels.filter(sub_vessels.distance_kilometers > 1000).show()` the TimeoutError persists. Unfortunately the solution wasn't found and some other means of filtering suspicious coordinates should be exmployed. 