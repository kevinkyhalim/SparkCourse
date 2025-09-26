from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel-names.txt")

# throw into a single value
lines = spark.read.text("Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# TODO: Superheroes with only 1 connection
Only1Connection = connections.filter(func.col("connections") == 1)

Only1ConnectionName = Only1Connection.join(names, "id")

Only1ConnectionName.show(Only1ConnectionName.count())

# TODO: Superheroes with the least amount of connection
# returns an integer with the minimum count of connections
minConnectionCount = connections.agg(func.min("connections")).first()[0]

LeastConnection = connections.filter(func.col("connections") == minConnectionCount)

LeastConnectionNames = LeastConnection.join(names, "id")

LeastConnectionNames.select("name").show()