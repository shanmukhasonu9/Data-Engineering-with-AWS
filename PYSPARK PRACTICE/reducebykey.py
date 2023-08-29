#reduceByKey() transformation is applied to the RDD rdd. The lambda function takes two values (a and b) and sums them up, effectively aggregating the counts for each word. The reduceByKey() transformation groups the RDD elements by key (word) and applies the reduction function (sum) to aggregate the counts.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('Project', 1),
('Gutenberg’s', 1),
('Alice’s', 1),
('Adventures', 1),
('in', 1),
('Wonderland', 1),
('Project', 1),
('Gutenberg’s', 1),
('Adventures', 1),
('in', 1),
('Wonderland', 1),
('Project', 1),
('Gutenberg’s', 1)]

rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.reduceByKey(lambda a,b: a+b)
for element in rdd2.collect():
    print(element)