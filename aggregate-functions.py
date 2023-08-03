import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
  
  
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)



# approx_count_distinct: Returns the approximate number of distinct values in the "salary" column.
print("approx_count_distinct: " + str(df.select(approx_count_distinct("salary")).collect()[0][0]))

# avg: Returns the average value of the "salary" column.
print("avg: " + str(df.select(avg("salary")).collect()[0][0]))

# collect_list: Collects all values of the "salary" column as a list.
df.select(collect_list("salary")).show(truncate=False)

# collect_set: Collects all unique values of the "salary" column as a set.
df.select(collect_set("salary")).show(truncate=False)

# countDistinct: Counts the distinct values of "department" and "salary" together.
df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: " + str(df2.collect()[0][0]))

# count: Returns the total number of rows in the DataFrame.
print("count: " + str(df.select(count("salary")).collect()[0][0]))

# first: Returns the first value of the "salary" column.
df.select(first("salary")).show(truncate=False)

# last: Returns the last value of the "salary" column.
df.select(last("salary")).show(truncate=False)

# kurtosis: Returns the kurtosis of the "salary" column.
df.select(kurtosis("salary")).show(truncate=False)

# max: Returns the maximum value of the "salary" column.
df.select(max("salary")).show(truncate=False)

# min: Returns the minimum value of the "salary" column.
df.select(min("salary")).show(truncate=False)

# mean: Returns the mean value of the "salary" column.
df.select(mean("salary")).show(truncate=False)

# skewness: Returns the skewness of the "salary" column.
df.select(skewness("salary")).show(truncate=False)

# stddev, stddev_samp, stddev_pop: Return the standard deviation of the "salary" column. 
df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show(truncate=False)

# sum: Returns the sum of values in the "salary" column.
df.select(sum("salary")).show(truncate=False)

# sumDistinct: Returns the sum of distinct values in the "salary" column.
df.select(sumDistinct("salary")).show(truncate=False)

# variance, var_samp, var_pop: Return the variance of the "salary" column. 
df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show(truncate=False)
