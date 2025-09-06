# Spark SQL

DataFrames vs DataSets

DataFrames are DataSet of Row objects

DataFrames allow for better interoperability and simplifies development (can perform most SQL operations on a DataFrame with one line)

Commands for DataFrames object:
- show()
- select("someFieldName")
- filter
- groupBy
- map

- printSchema()
- count

- explode() similar to flatmap; explodes columns into rows
- split()
- lower()

- StructField() - create an explicit schema
- withColumn() - add a new column

## Pandas Integration with Spark
- transform() for when returns are of the same length as inputs
- apply() for when returns are of different lenghts
- transform_batch() & apply_batch() for batch processing

## UDTableFs
- UDF allows custom logic to be applied row by row in Spark SQL / DataFrames
- UDTFs allow one input row to return multiple output rows and columns
- Used to flatten nested events from JSON event logs & extracting multiple rows per input row, replace explode() with something more efficient