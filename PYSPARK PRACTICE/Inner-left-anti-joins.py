import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('machinelearninggeeks.com').getOrCreate()

# Defining the data and column names for the empDF DataFrame
emp = [(1, "Smith", -1, "2018", "10", "M", 3000), \
       (2, "Rose", 1, "2010", "20", "M", 4000), \
       (3, "Williams", 1, "2010", "10", "M", 1000), \
       (4, "Jones", 2, "2005", "10", "F", 2000), \
       (5, "Brown", 2, "2010", "40", "", -1), \
       (6, "Brown", 2, "2010", "50", "", -1) \
      ]
empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]

# Creating the empDF DataFrame
empDF = spark.createDataFrame(data=emp, schema=empColumns)

# Printing the schema and content of empDF DataFrame
empDF.printSchema()
empDF.show(truncate=False)

# Defining the data and column names for the deptDF DataFrame
dept = [("Finance", 10), \
        ("Marketing", 20), \
        ("Sales", 30), \
        ("IT", 40) \
       ]
deptColumns = ["dept_name", "dept_id"]

# Creating the deptDF DataFrame
deptDF = spark.createDataFrame(data=dept, schema=deptColumns)

# Printing the schema and content of deptDF DataFrame
deptDF.printSchema()
deptDF.show(truncate=False)

# Inner join (default join type) between empDF and deptDF on emp_dept_id and dept_id columns
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "left").show(truncate=False)

# Left outer join between empDF and deptDF on emp_dept_id and dept_id columns
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftouter").show(truncate=False)

# Performing a left anti join between empDF and deptDF on emp_dept_id and dept_id columns
joinDF2 = spark.sql("SELECT e.* FROM EMP e LEFT ANTI JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)
