#code demonstrates how to cast a column in a Spark DataFrame to a different data type using the cast function.
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

# Define data and DataFrame columns
simpleData = [
    ("James", "34", "true", "M", "3000.6089"),
    ("Michael", "33", "true", "F", "3300.8067"),
    ("Robert", "37", "false", "M", "5000.5034")
]

columns = ["firstname", "age", "isGraduated", "gender", "salary"]

# Create the DataFrame
df = spark.createDataFrame(data=simpleData, schema=columns)

# Print the DataFrame schema and data
df.printSchema()
df.show(truncate=False)


from pyspark.sql.functions import col

# Cast the "salary" column to DoubleType and print the schema
df.withColumn("salary", df.salary.cast('double')).printSchema()

# Cast the "salary" column to DoubleType and print the schema
df.withColumn("salary", df.salary.cast(DoubleType())).printSchema()

# Cast the "salary" column to DoubleType using col function and print the schema
df.withColumn("salary", col("salary").cast('double')).printSchema()
