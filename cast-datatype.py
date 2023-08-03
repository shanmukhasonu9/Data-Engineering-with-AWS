#below code demonstrates how to cast the data types of columns in a Spark DataFrame using various PySpark functions. The DataFrame df contains columns with different data types, and #the code converts those data types into desired types.
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James",34,"2006-01-01","true","M",3000.60),
    ("Michael",33,"1980-01-10","true","F",3300.80),
    ("Robert",37,"06-01-1992","false","M",5000.50)
  ]

columns = ["firstname","age","jobStartDate","isGraduated","gender","salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)


#Cast columns to desired data types using withColumn
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, BooleanType, DateType

# Cast "age" to StringType, "isGraduated" to BooleanType, and "jobStartDate" to DateType
df2 = df.withColumn("age", col("age").cast(StringType())) \
        .withColumn("isGraduated", col("isGraduated").cast(BooleanType())) \
        .withColumn("jobStartDate", col("jobStartDate").cast(DateType()))

# Print the DataFrame schema after casting
df2.printSchema()


#Use selectExpr to cast columns in the DataFrame
# Cast "age" to IntegerType, "isGraduated" to StringType, and "jobStartDate" to StringType using selectExpr
df3 = df2.selectExpr("cast(age as int) age",
                     "cast(isGraduated as string) isGraduated",
                     "cast(jobStartDate as string) jobStartDate")

# Print the DataFrame schema after casting
df3.printSchema()
df3.show(truncate=False)


# Create a temporary view
df3.createOrReplaceTempView("CastExample")

# Use Spark SQL to cast columns to IntegerType, BooleanType, and DateType
df4 = spark.sql("SELECT STRING(age), BOOLEAN(isGraduated), DATE(jobStartDate) from CastExample")

# Print the DataFrame schema after casting
df4.printSchema()
df4.show(truncate=False)
