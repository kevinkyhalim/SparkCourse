# Demonstrating key value pairs RDD
from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# load the data
filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "fakefriends.csv"))
lines = sc.textFile(filepath)
rdd = lines.map(parseLine)

# mapValue leaves the keys untouched and adds a 1 after each of the values (in this case numFriends)
# reduceByKey aggregates based on the key value and executes the function in lambda
# x[0] is the numFriends value, x[1] is the 1 value from mapValues
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# collects the data from the pipelineRDD object averagesByAge
results = averagesByAge.collect()
for result in results:
    print(result)