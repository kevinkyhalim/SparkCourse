from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "fakefriends-header.csv"))
# no need to use SparkContext
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(filepath)

# Discard information that is not needed as early as possible
friendsByAge = people.select("age", "friends")

#F From friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# Sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# Formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()

# With a custome column name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.stop()

