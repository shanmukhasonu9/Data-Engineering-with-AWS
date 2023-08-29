#The map() transformation in PySpark is used to apply a function to each element in a dataset. This function takes a single element as input and returns a transformed element as output. The map() transformation returns a new dataset that consists of the transformed elements.
#mapPartitions() is mainly used to initialize connections once for each partition instead of every row, this is the main difference between map() vs mapPartitions().

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

#Example 1 mapPartitions()
def reformat(partitionData):
    for row in partitionData:
        yield [row.firstname+","+row.lastname,row.salary*10/100]
df.rdd.mapPartitions(reformat).toDF().show()

output:
+---------------+-----+
|             _1|   _2|
+---------------+-----+
|    James,Smith|300.0|
|      Anna,Rose|410.0|
|Robert,Williams|620.0|
+---------------+-----+
