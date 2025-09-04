from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "book.txt"))
input = sc.textFile(filepath)
words = input.flatMap(lambda x: x.split())
# count of how many times a value occurs
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
