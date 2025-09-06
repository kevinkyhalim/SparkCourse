from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os

spark = SparkSession.builder.appName("WordCount").getOrCreate()

filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "book.txt"))
# Read each line of my book into a dataframe
inputDF = spark.read.text(filepath)

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())
