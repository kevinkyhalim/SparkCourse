from pyspark import SparkConf, SparkContext
import collections
import os

# sets master node as the local machine, no distribution in this case
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# creates the Spark Context object
sc = SparkContext(conf = conf)

# load the data
filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "ml-100k", "u.data"))
# breaks input file line by line
lines = sc.textFile(filepath)
# takes the 3rd value after each line is splitted
ratings = lines.map(lambda x: x.split()[2])
# count the ratings
result = ratings.countByValue()

# All python code below
# create an ordered dictionary sorted on the items of the result object
sortedResults = collections.OrderedDict(sorted(result.items()))
# print rating and count of rating
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
