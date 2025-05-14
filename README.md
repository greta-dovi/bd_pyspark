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
Total travelled distance was calculated by grouping values based on their MMSI and summing the distances. The final results were sorted in descending order. <br>
The 
<!-- Tasks


Data Processing with PySpark
Calculate the distance between consecutive positions for each vessel using a suitable geospatial library or custom function that can integrate with PySpark.
Aggregate these distances by MMSI to get the total distance traveled by each vessel on that day.

Identifying the Longest Route
Sort or use an aggregation function to determine which vessel traveled the longest distance.

Output
The final output should be the MMSI of the vessel that traveled the longest distance, along with the computed distance.

Code Documentation and Comments
Ensure the code is well-documented, explaining key PySpark transformations and actions used in the process.

Deliverables
A PySpark script that completes the task from loading to calculating and outputting the longest route.
A brief report or set of comments within the code that discusses the findings and any interesting insights about the data or the computation process. -->
Errors