#it creates a DataFrame with different data types and then performs operations like exploding an array, splitting strings, creating new arrays, and checking for array containment.

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType,StructType,StructField
spark = SparkSession.builder \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()


arrayCol = ArrayType(StringType(),False)

data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True) 
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()


#explode function from PySpark to transform the array elements in the "languagesAtSchool" column into separate rows. 
from pyspark.sql.functions import explode
df.select(df.name,explode(df.languagesAtSchool)).show()

#split function from PySpark to split the "name" column into an array of strings based on the comma (,) delimiter.
from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()

#array function from PySpark to create a new array column named "States" by combining the values from the "currentState" and "previousState" columns.
from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()

#array_contains function from PySpark to check if the "languagesAtSchool" array column contains the value "Java" for each row.
from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java")
    .alias("array_contains")).show()