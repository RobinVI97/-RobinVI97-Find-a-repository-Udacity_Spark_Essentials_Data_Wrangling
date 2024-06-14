# Take care of any imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Create the Spark Context
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()
# Complete the script
path = "/home/workspace/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
user_log = spark.read.json(path)


# # Data Exploration 
# 
# # Explore the data set.


# View 5 records 
##print(user_log.take(5))

# Print the schema
##print(user_log.printSchema())

# Describe the dataframe
##print(user_log.describe())

# Describe the statistics for the song length column
##user_log.describe("length").show()

# Count the rows in the dataframe
##print(user_log.count())    


# Select the page column, drop the duplicates, and sort by page
##user_log.select("page").dropDuplicates().sort("page").show()

# Select data for all pages where userId is 1046
user_log.select(["userId", "firstname", "page", "song"]).where(user_log.userId == '1046').show()

# # Calculate Statistics by Hour


# Select just the NextSong page


# # Drop Rows with Missing Values


# How many are there now that we dropped rows with null userId or sessionId?


# select all unique user ids into a dataframe


# Select only data for where the userId column isn't an empty string (different from null)


# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 



# Create a user defined function to return a 1 if the record contains a downgrade


# Select data including the user defined function



# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.



# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have



# Show the phases for user 1138 

