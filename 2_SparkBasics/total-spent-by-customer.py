# csv files contains
# customerid, itemid, totalspent
# Objective: find total spent by customer

from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    custID = int(fields[0])
    dollarspent = float(fields[2])
    return (custID, dollarspent)

# load the data
filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "customer-orders.csv"))
lines = sc.textFile(filepath)
# 1. split each csv line into a field
# 2. map each line to key/value pairs of custid and total spent
input = lines.map(extractCustomerPricePairs)
# 3. reduceByKey to add up amount spent by cust ID
totalspentbyID = input.reduceByKey(lambda x, y: x + y)

totspentbyIDSorted = totalspentbyID.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

# 4. collect() results and print!
results = totspentbyIDSorted.collect()
for totspent, custID in results:
    print (custID, round(totspent,2))