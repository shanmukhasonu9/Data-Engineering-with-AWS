#importing spark session
from pyspark.sql import SparkSession
# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples').getOrCreate()

# Create DataFrame
data = [("James","Smith","USA","CA"),("Michael","Rose","USA","NY"), \
    ("Robert","Williams","USA","CA"),("Maria","Jones","USA","FL") \
  ]
columns=["firstname","lastname","country","state"]
df=spark.createDataFrame(data=data,schema=columns)
df.show()
print(df.collect())


# Extract states using DataFrame transformations
states1=df.rdd.map(lambda x: x[3]).collect()
print(states1)




