# Databricks notebook source
# MAGIC %md
# MAGIC **RDD Creation**

# COMMAND ----------

# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 

# Create RDD from external Data source
rdd2 = spark.sparkContext.textFile("/path/test.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Dataframe**

# COMMAND ----------

# Create DataFrame
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dataframe Operations
# MAGIC - Rename column on DataFrame
# MAGIC - Add column to DataFrame
# MAGIC - Filter rows from DataFrame
# MAGIC - Sort DataFrame Rows
# MAGIC - Using xplode array and map columns to rows
# MAGIC - Explode nested array into rows

# COMMAND ----------

# MAGIC %md
# MAGIC - ## Rename Dataframe columns
# MAGIC
# MAGIC   01.PySpark withColumnRenamed – To rename DataFrame column name
# MAGIC
# MAGIC   02.PySpark withColumnRenamed – To rename multiple columns
# MAGIC
# MAGIC   03.Using StructType – To rename nested column on PySpark DataFrame
# MAGIC
# MAGIC   04.Using Select – To rename nested columns
# MAGIC
# MAGIC   05.Using withColumn – To rename nested columns
# MAGIC
# MAGIC   06.Using col() function – To Dynamically rename all or multiple columns
# MAGIC
# MAGIC   07.Using toDF() – To rename all or multiple columns
# MAGIC

# COMMAND ----------


dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data = dataDF, schema = schema)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **01.PySpark withColumnRenamed – To rename DataFrame column name**
# MAGIC
# MAGIC PySpark has a withColumnRenamed() function on DataFrame to change a column name. This is the most straight forward approach; this function takes two parameters; the first is your existing column name and the second is the new column name you wish for.
# MAGIC

# COMMAND ----------

df.withColumnRenamed("dob","DateOfBirth").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **02.PySpark withColumnRenamed – To rename multiple columns**
# MAGIC
# MAGIC To change multiple column names, we should chain withColumnRenamed functions as shown below. You can also store all columns to rename in a list and loop through to rename all columns

# COMMAND ----------

df2 = df.withColumnRenamed("dob","DateOfBirth") \
    .withColumnRenamed("salary","salary_amount")
df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **03.Using PySpark StructType – To rename a nested column in Dataframe**
# MAGIC
# MAGIC Changing a column name on nested data is not straight forward and we can do this by creating a new schema with new DataFrame columns using StructType and use it using cast function

# COMMAND ----------

from pyspark.sql.functions import col

schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())])

df.select(col("name").cast(schema2), \
     col("dob"), col("gender"),col("salary")) \
   .printSchema()  

# COMMAND ----------

# MAGIC %md
# MAGIC **04.Using Select – To rename nested elements.**
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("name.firstname").alias("fname"), \
  col("name.middlename").alias("mname"), \
  col("name.lastname").alias("lname"), \
  col("dob"),col("gender"),col("salary")) \
  .printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **05.Using PySpark DataFrame withColumn – To rename nested columns**
# MAGIC
# MAGIC When you have nested columns on PySpark DatFrame and if you want to rename it, use withColumn on a data frame object to create a new column from an existing and we will need to drop the existing column. 

# COMMAND ----------

from pyspark.sql.functions import *
df4 = df.withColumn("fname",col("name.firstname")) \
      .withColumn("mname",col("name.middlename")) \
      .withColumn("lname",col("name.lastname")) \
      .drop("name")
df4.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **06.Using col() function – To Dynamically rename all or multiple columns**

# COMMAND ----------

old_columns = ["dob","gender","salary","fname","mname","lname"]
new_columns = ["DateOfBirth","Sex","salary","firstName","middleName","lastName"]
columnsList = [col(old).alias(new) for old, new in zip(old_columns, new_columns)]
df5 = df4.select(columnsList)
df5.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **07.Using toDF() – To change all columns in a PySpark DataFrame**

# COMMAND ----------

newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(*newColumns).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark withcolumn
# MAGIC - PySpark withColumn – To change column DataType
# MAGIC - Transform/change value of an existing column
# MAGIC - Derive new column from an existing column
# MAGIC - Add a column with the literal value
# MAGIC - Rename column name
# MAGIC - Drop DataFrame column

# COMMAND ----------

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema = columns)

# COMMAND ----------

# MAGIC %md
# MAGIC **01.Change DataType using PySpark withColumn()**
# MAGIC
# MAGIC By using PySpark withColumn() on a DataFrame, we can cast or change the data type of a column. In order to change data type, you would also need to use cast() function along with withColumn().

# COMMAND ----------

from pyspark.sql.functions import *
df.withColumn("salary",col("salary").cast("Integer")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC **02.Update The Value of an Existing Column**
# MAGIC
# MAGIC PySpark withColumn() function of DataFrame can also be used to change the value of an existing column. In order to change the value, pass an existing column name as a first argument and a value to be assigned as a second argument to the withColumn() function. Note that the second argument should be Column type 

# COMMAND ----------

df.withColumn("salary",col("salary")*100).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **03.Create a Column from an Existing**
# MAGIC
# MAGIC To add/create a new column, specify the first argument with a name you want your new column to be and use the second argument to assign a value by applying an operation on an existing column.

# COMMAND ----------

df.withColumn("CopiedColumn",col("salary")* -1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **04.Add a New Column using withColumn()**
# MAGIC
# MAGIC In order to create a new column, pass the column name you wanted to the first argument of withColumn() transformation function. Make sure this new column not already present on DataFrame, if it presents it updates the value of that column.

# COMMAND ----------

df.withColumn("Country", lit("USA")).show()
df.withColumn("Country", lit("USA")) \
  .withColumn("anotherColumn",lit("anotherValue")) \
  .show()

# COMMAND ----------

# MAGIC %md
# MAGIC **05.Rename Column Name**
# MAGIC
# MAGIC Though you cannot rename a column using withColumn, still I wanted to cover this as renaming is one of the common operations we perform on DataFrame. To rename an existing column use withColumnRenamed() function on DataFrame.

# COMMAND ----------

df.withColumnRenamed("gender","sex") \
  .show(truncate=False) 

# COMMAND ----------

# MAGIC %md
# MAGIC **06. Drop Column From PySpark DataFrame**
# MAGIC

# COMMAND ----------

df.drop("salary") \
  .show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark Dataframe using Filter

# COMMAND ----------

# Imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType

# Create SparkSession object
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Create data
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]

# Create schema        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

# Create dataframe
df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **DataFrame filter() with Column Condition**

# COMMAND ----------

# Using equal condition
df.filter(df.state == "OH").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Using not equal filter condition**
# MAGIC
# MAGIC 01.Using != operator 
# MAGIC
# MAGIC 02.Using ~ (Negation) operator
# MAGIC
# MAGIC 03.Using col() Function

# COMMAND ----------

# Not equals condition
df.filter(df.state != "OH") \
    .show(truncate=False) 

# Another expression
df.filter(~(df.state == "OH")) \
    .show(truncate=False)

# Using SQL col() function
from pyspark.sql.functions import col
df.filter(col("state") == "OH") \
    .show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Filtering with SQL Expression**

# COMMAND ----------

# Using SQL Expression
df.filter("gender == 'M'").show()

# For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **PySpark Filter with Multiple Conditions**

# COMMAND ----------

# Filter multiple conditions
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False)  

# COMMAND ----------

# MAGIC %md
# MAGIC **Filter Based on List Values**

# COMMAND ----------

# Filter IS IN List values
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Filter Based on Starts With, Ends With, Contains**

# COMMAND ----------


# Using startswith
df.filter(df.state.startswith("N")).show()

#using endswith
df.filter(df.state.endswith("H")).show()

#contains
df.filter(df.state.contains("H")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Filtering with Regular Expression**

# COMMAND ----------

# Prepare Data
data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()

# rlike - SQL RLIKE pattern (LIKE with Regex)
# This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Filtering Array column**

# COMMAND ----------

# Using array_contains()
from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)  

# COMMAND ----------

# MAGIC %md
# MAGIC **Filtering on Nested Struct columns**

# COMMAND ----------

# Struct condition
df.filter(df.name.lastname == "Williams") \
    .show(truncate=False) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark - How sort a Dataframe
# MAGIC
# MAGIC - Using sort() function
# MAGIC - Using orderBy() function
# MAGIC - Ascending order
# MAGIC - Descending order
# MAGIC - SQL Sort functions

# COMMAND ----------

# Imports
import pyspark
from pyspark.sql import SparkSession

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
# Create SparkSession

df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **01.DataFrame sorting using the sort() function**
# MAGIC
# MAGIC PySpark DataFrame class provides sort() function to sort on one or more columns. sort() takes a Boolean argument for ascending or descending order. To specify different sorting orders for different columns, you can use the parameter as a list.

# COMMAND ----------

# Sorting DataFrame using sort()

df.sort("department","state").show(truncate=False)
df.sort(col("department"),col("state")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **02.DataFrame sorting using orderBy() function**
# MAGIC
# MAGIC PySpark DataFrame also provides orderBy() function to sort on one or more columns.

# COMMAND ----------

# Sorting DataFrame using orderBy()

df.orderBy("department","state").show(truncate=False)
df.orderBy(col("department"),col("state")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **03.Sort by Ascending (ASC)**
# MAGIC
# MAGIC The sort() method allows you to specify the sorting column(s) and the sorting order (ascending or descending). If you want to specify the ascending order/sort explicitly on DataFrame, you can use the asc method of the Column function.

# COMMAND ----------

# Sort DataFrame with asc

df.sort(df.department.asc(),df.state.asc()).show(truncate=False)
df.sort(col("department").asc(),col("state").asc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **04.Sort by Descending (DESC)**
# MAGIC
# MAGIC If you want to specify the sorting by descending order on DataFrame, you can use the desc method of the Column function.

# COMMAND ----------

# Sort DataFrame with desc

df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
df.sort(col("department").asc(),col("state").desc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **06.Using Raw SQL**

# COMMAND ----------

# Sort using spark SQL

df.createOrReplaceTempView("EMP")
spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark - Explode Array and Map columns to rows
# MAGIC
# MAGIC 01.explode()
# MAGIC
# MAGIC 02.explode_outer()
# MAGIC
# MAGIC 03.posexplode()
# MAGIC
# MAGIC 04.posexplode_outer()
# MAGIC

# COMMAND ----------

# Create SparkSession and Prepare sample Data
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **01.explode() – PySpark explode array or map column to rows**
# MAGIC
# MAGIC PySpark function explode(e: Column) is used to explode or create array or map columns to rows. When an array is passed to this function, it creates a new default column “col1” and it contains all array elements. When a map is passed, it creates two new columns one for key and one for value and each element in map split into the rows.
# MAGIC
# MAGIC This will ignore elements that have null or empty.

# COMMAND ----------

# explode() on array column
from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **1.2  explode – map column example**

# COMMAND ----------

# explode() on map column
from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties))
df3.printSchema()
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **02. explode_outer() – Create rows for each element in an array or map.**
# MAGIC
# MAGIC PySpark SQL explode_outer(e: Column) function is used to create a row for each element in the array or map column. Unlike explode, if the array or map is null or empty, explode_outer returns null.

# COMMAND ----------

# explode_outer() on array and map column
from pyspark.sql.functions import explode_outer
df.select(df.name,explode_outer(df.knownLanguages)).show()
df.select(df.name,explode_outer(df.properties)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **03. posexplode() – explode array or map elements to rows**
# MAGIC
# MAGIC posexplode(e: Column) creates a row for each element in the array and creates two columns “pos’ to hold the position of the array element and the ‘col’ to hold the actual array value. And when the input column is a map, posexplode function creates 3 columns “pos” to hold the position of the map element, “key” and “value” columns.
# MAGIC
# MAGIC This will ignore elements that have null or empty.
# MAGIC
# MAGIC

# COMMAND ----------

# posexplode() on array and map
from pyspark.sql.functions import posexplode
df.select(df.name,posexplode(df.knownLanguages)).show()
df.select(df.name,posexplode(df.properties)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **04.posexplode_outer() – explode array or map columns to rows.**
# MAGIC
# MAGIC Spark posexplode_outer(e: Column) creates a row for each element in the array and creates two columns “pos’ to hold the position of the array element and the ‘col’ to hold the actual array value. Unlike posexplode, if the array or map is null or empty, posexplode_outer function returns null, null for pos and col columns. Similarly for the map, it returns rows with nulls.

# COMMAND ----------

# posexplode_outer() on array and map 
from pyspark.sql.functions import posexplode_outer
from pyspark.sql.functions import col 
df.select(col("name"), posexplode_outer(col("knownLanguages"))).show()

df.select(df.name,posexplode_outer(df.properties)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pyspark - Explode Nested Array into Rows

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------


from pyspark.sql.functions import explode
df.select(df.name,explode(df.subjects)).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import flatten
df.select(df.name,flatten(df.subjects)).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC