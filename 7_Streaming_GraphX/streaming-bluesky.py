# Goal: use a rate source to simulate 50 random posts per second
# Write a UDTF to extract the hastags from each post
# Use SparkSQL and LATERAL to apply the UDTF to each post
# Apply an additional Spark SQL query to keep track of the top 10 hastags over time

import random
import os
import re
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, udtf
from pyspark.sql.types import StringType, IntegerType

JSONL_FILE = "./bluesky.jsonl"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SparkStreamingBlueskySimulator") \
    .getOrCreate()

# modify the random_log_line to get a random json_text_line
@udf(StringType())
def get_random_jsonl_text():
    """Returns a random text from the jsonl file without reading the entire file into memory."""
    try:
        if not os.path.exists(JSONL_FILE):
            return None  # Handle the case where the json file is missing

        file_size = os.path.getsize(JSONL_FILE)
        if file_size == 0:
            return None  # Handle empty file scenario

        with open(JSONL_FILE, "r",  encoding='utf-8') as lf:
            while True:
                random_position = random.randint(0, file_size - 1)  # Pick a random position
                lf.seek(random_position)  # Jump to that position
                lf.readline()  # Discard partial line (move to next full line)
                line = lf.readline().strip()  # Read a full line
                # can also put this under the HashtagExtractor
                data = json.loads(line)
                text = data.get('text', "")
            
                if text and str(text).strip():
                    return str(text).strip()
                else:
                    return None

    except Exception as e:
        print(str(e))
        return None

@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        """Extracts hashtags from the input text."""
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag.lower(),)

# Register the UDTF for use in Spark SQL
spark.udtf.register("extract_hashtags", HashtagExtractor)

# Create Streaming DataFrame from rate source
rate_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 50) \
    .load()

# Enrich DataFrame with canned log data
postLines = rate_df.withColumn("value", get_random_jsonl_text())

# Register DataFrame as a temporary table called raw_posts
postLines.createOrReplaceTempView("raw_posts")

# Use SQL to extract structured log fields
structured_jsonl_query = """
    SELECT
        value AS text
    FROM raw_posts
"""

logsDF = spark.sql(structured_jsonl_query)

# Register logsDF as a SQL table for further queries
logsDF.createOrReplaceTempView("bluesky_text")

# SQL Query: Keep track of the top hastags encountered over time
topHashtagsDF = spark.sql("""
    SELECT hashtag, COUNT(*) as num_of_hashtags
    FROM bluesky_text, LATERAL extract_hashtags(text)
    WHERE hashtag IS NOT NULL 
    GROUP BY hashtag
    ORDER BY num_of_hashtags DESC
    LIMIT 10
""")

# Kick off our streaming query, dumping top user agents to the console
query = (topHashtagsDF.writeStream
         .outputMode("complete")
         .format("console")
         .queryName("top_user_agents")
         .start())

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()
