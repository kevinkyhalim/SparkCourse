# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:15:05 2019

@author: Frank
"""
# Keep track of the most viewed URL's (endpoints) in our logs
# 30 second windo and 10 second slide
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql import functions as func
from pyspark.sql.functions import regexp_extract

# Create a SparkSession
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
# The spark will keep reading the logs folder for more updates!
accessLines = spark.readStream.text("logs")


# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())
# Keep a running count of every access by status code
# statusCountsDF = logsDF.groupBy(logsDF.status).count()
EndpointsCountsDF = logsDF2.groupBy(func.window(func.col("eventTime"),windowDuration="30 seconds", slideDuration = "10 seconds"), func.col("endpoint")).count()

sortedEndpointCounts = EndpointsCountsDF.orderBy(func.col("count").desc())
# Kick off our streaming query, dumping results to the console
query = ( sortedEndpointCounts.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

