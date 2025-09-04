# Demonstrating Filtering in RDD

from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # convert temperature from Celcius to Fahrenheit
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "1800.csv"))
lines = sc.textFile(filepath)
parsedLines = lines.map(parseLine)

# throws away all lines that does not correspond to the filter
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# convert to a key, value pair
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))