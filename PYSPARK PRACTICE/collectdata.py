#code demonstrates how to create a Spark DataFrame, collect data from it, and perform basic operations on the collected data.
import pyspark
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Define data and DataFrame columns for the "dept" dataset
dept = [
    ("Finance", 10),
    ("Marketing", 20),
    ("Sales", 30),
    ("IT", 40)
]
deptColumns = ["dept_name", "dept_id"]

# Create the DataFrame
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)

# Print the DataFrame schema and data
deptDF.printSchema()
deptDF.show(truncate=False)


dataCollect = deptDF.collect()

# Print the collected data
print(dataCollect)


dataCollect2 = deptDF.select("dept_name").collect()

# Print the collected data
print(dataCollect2)

for row in dataCollect:
    print(row['dept_name'] + "," + str(row['dept_id']))
