from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os

spark = SparkSession.builder.appName("TotalSpentbyCustID").getOrCreate()

schema = StructType([ \
                     StructField("custID", StringType(), True), \
                     StructField("itemID", StringType(), True), \
                     StructField("total", FloatType(), True)])

# // Read the file as dataframe
filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), "customer-orders.csv"))
df = spark.read.schema(schema).csv(filepath)
df.printSchema()

# Take out the itemID column
spent_per_customer = df.select("custID", "total")

total_spent_by_customer = spent_per_customer.groupBy("custID").agg(func.round(func.sum("total"),2).alias("total_spent")).sort("total_spent")

total_spent_by_customer.show(total_spent_by_customer.count())
    
spark.stop()

                                                  