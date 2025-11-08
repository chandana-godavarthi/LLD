# Databricks notebook source
# MAGIC %md
# MAGIC ## Question 1

# COMMAND ----------

# 1. Write PySpark code to get top 2 products by amount per user.

# +--------+----------+-------+
# |user_id |product_id|amount |
# +--------+----------+-------+
# |1       |101       |200    |
# |1       |102       |500    |
# |1       |103       |300    |
# |2       |101       |400    |
# |2       |102       |100    |
# +--------+----------+-------+

# COMMAND ----------

# MAGIC %md
# MAGIC - Step 1: Created DataFrame using sample data given.
# MAGIC - Step 2: Window partitioned by user, ordered by amount desc.
# MAGIC - Step 3: created Rank using row_number and filtered rank < 3.

# COMMAND ----------

# DBTITLE 1,Created dataframe using sample data given
data=[
    (1,101,200),
    (1,102,500),
    (1,103,300),
    (2,101,400),
    (2,102,100)
]
columns=['user_id','product_id','amount']
df = spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

# DBTITLE 1,Created Rank Column using Row_Number
from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number
window_spec = Window.partitionBy("user_id").orderBy(col("amount").desc())
df_rank = df.withColumn("rank",row_number().over(window_spec))
df_rank.display()

# COMMAND ----------

# DBTITLE 1,Step 3 - Filtered Rank < 3
df_final = df_rank.filter(col("rank") < 3).select("user_id","product_id")
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 2

# COMMAND ----------

# 2. Given Sales data 

# +--------+-------+------+
# |region  |month  |sales |
# +--------+-------+------+
# |East    |Jan    |100   |
# |East    |Feb    |120   |
# |West    |Jan    |80    |
# +--------+-------+------+

# Transform to this 

# +--------+-----+-----+
# |region  |Jan  |Feb  |
# +--------+-----+-----+
# |East    |100  |120  |
# |West    |80   |NULL |
# +--------+-----+-----+

# COMMAND ----------

# MAGIC %md
# MAGIC - Step 1: Created DataFrame using sample data given.
# MAGIC - Step 2: Using SQL **case `when`** and **GroupBY**
# MAGIC - Step 3: Using Pyspark Built in **PIVOT**  function

# COMMAND ----------

# DBTITLE 1,Created df using sample data given
data=[
    ('East','Jan',100),
    ('East','Feb',120),
    ('West','Jan',80)
]
columns = ['region','month','sales']
df = spark.createDataFrame(data,columns)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Group By** region and for each region if Month sum up all sales for partitcular month and create month column using **CASE WHEN** 

# COMMAND ----------

# DBTITLE 1,Using SQL CASE WHEN
df.createOrReplaceTempView("df")
df_final = spark.sql("""
                     SELECT
  region,
  SUM(CASE WHEN month = 'Jan' THEN sales ELSE NULL END) AS Jan,
  SUM(CASE WHEN month = 'Feb' THEN sales ELSE NULL END) AS Feb
FROM df
GROUP BY region
                    """)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Using Pyspark built in **PIVOT** function

# COMMAND ----------

# DBTITLE 1,Using Pyspark PIVOT
from pyspark.sql.functions import sum,when
df_pivot = df.groupBy("region").pivot("month").agg(sum("sales"))
df_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 3

# COMMAND ----------

# 3. Parse the JSON, explode the events array, and create a DataFrame with columns: user_id, event_type, timestamp

# # Sample DataFrame
# data = [
#     ('{"user_id":1,"events":[{"event_type":"click","timestamp":"2023-01-01T10:00:00"},{"event_type":"purchase","timestamp":"2023-01-01T10:05:00"}]}',),
#     ('{"user_id":2,"events":[{"event_type":"click","timestamp":"2023-01-02T11:00:00"}]}',)
# ]

# COMMAND ----------

# MAGIC %md
# MAGIC - Step 1:  Created raw string DF using sampple given.
# MAGIC - Step 2: Defined schema for JSON.
# MAGIC - Step 3: Parsed json string with **from_json**.
# MAGIC - Step 4: using **Explode** converted event array to each row per user.
# MAGIC - Step 5: Selected event_type and timestamp from the event object.

# COMMAND ----------

# DBTITLE 1,Created df using data given
data = [
    ('{"user_id":1,"events":[{"event_type":"click","timestamp":"2023-01-01T10:00:00"},{"event_type":"purchase","timestamp":"2023-01-01T10:05:00"}]}',),
    ('{"user_id":2,"events":[{"event_type":"click","timestamp":"2023-01-02T11:00:00"}]}',)
]
df = spark.createDataFrame(data,["json_str"])
df.display()

# COMMAND ----------

# DBTITLE 1,parsing json string using FROM_JSON
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("events", ArrayType(
        StructType([
            StructField("event_type", StringType()),
            StructField("timestamp", StringType())
        ])
    ))
])
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import from_json, col

df_parsed = df.withColumn("parsed", from_json(col("json_str"), schema))
df_parsed.display()

# COMMAND ----------

# DBTITLE 1,converting array elements to rows using EXPLODE
from pyspark.sql.functions import explode

df_exploded = df_parsed.select(
    col("parsed.user_id").alias("user_id"),
    explode(col("parsed.events")).alias("event")
)
df_exploded.display()

# COMMAND ----------

# DBTITLE 1,displaying parsed results
df_final = df_exploded.select(
    "user_id",
    col("event.event_type"),
    col("event.timestamp")
)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 4

# COMMAND ----------

# 4. Merge (Upsert) New JSON Data into Existing Table

# user_id	name	age
# 1		Alice	30
# 2		Bob		25

# New data : 
# {"user_id":1, "name":"Alice", "age":31}
# {"user_id":3, "name":"Charlie", "age":22}

# COMMAND ----------

# MAGIC %md
# MAGIC - Step 1: Create DataFrame and save as Delta table.
# MAGIC - Step 2: Read new JSON data into new DataFrame.
# MAGIC - Step 3: Merge on user_id (update + insert).

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS df_test

# COMMAND ----------

# DBTITLE 1,Create delta table using given data
schema = StructType([
    StructField("user_id",IntegerType()),
    StructField("name",StringType()),
    StructField("age",IntegerType())
])
data = [
    (1, "Alice", 30),
    (2, "Bob", 25)
]
df = spark.createDataFrame(data,schema)
df.write.format("delta").mode("overwrite").saveAsTable("df_test")
df = spark.read.table("df_test")
df.display()

# COMMAND ----------

# DBTITLE 1,create new df using new json data
# New JSON
data = [
    ('{"user_id":1, "name":"Alice", "age":31}',),
    ('{"user_id":3, "name":"Charlie", "age":22}',)
]
df_new_raw = spark.createDataFrame(data, ["json_str"])
df_new_raw.display()

# COMMAND ----------

# DBTITLE 1,parsing the new json data using from_json
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])
df_json = df_new_raw.select(from_json("json_str", schema).alias("parsed")).select("parsed.*")
df_json.display()
df_json.createOrReplaceTempView("df_raw")

# COMMAND ----------

# DBTITLE 1,merging data into delta table
df_merge = spark.sql("""
                     MERGE INTO df_test tgt
USING df_raw src
on tgt.user_id = src.user_id
WHEN MATCHED THEN
  UPDATE SET tgt.name = src.name, tgt.age = src.age
WHEN NOT MATCHED THEN
  INSERT (user_id, name, age)
  VALUES (src.user_id, src.name, src.age)
  """)
df_merge.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS df_test

# COMMAND ----------

# DBTITLE 1,Updated Target df_test table displayed
df_updated = spark.read.table("df_test")
df_updated.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 5

# COMMAND ----------

# 5.Find Patients with Increasing Sugar level

# Problem:
# You have patient monthly Sugar level details:
# Find users whose Sugar levels are strictly increasing every month.

# user_id	month	Value
# 1		2023-01	100
# 1		2023-02	130
# 1		2023-03	160
# 2		2023-01	170
# 2		2023-02	180
# 2		2023-03	150

# COMMAND ----------

# MAGIC %md
# MAGIC - Step 1: Create df using sample data
# MAGIC - Step 2: Create column prev_value by grouping based on user_id and taking user_id prev value using **lag**.
# MAGIC - Step 3: Create new column is_increasing and flag with value 1 if value > prev_value else 0
# MAGIC - Step 4: Get user_id whose sugar level did not increase and subtract them from df
# MAGIC - Step 5: Show all users with strictly increasing sugar levels excluding prev non increasing users

# COMMAND ----------

# DBTITLE 1,created df using given data
data = [
    (1,'2023-01',100),
    (1,'2023-02',130),
    (1,'2023-03',160),
    (2,'2023-01',170),
    (2,'2023-02',180),
    (2,'2023-03',150)
]
schema = StructType([
    StructField("user_id",IntegerType()),
    StructField("month",StringType()),
    StructField("Value",IntegerType())
])
df = spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

# DBTITLE 1,calculating prev_value using lag window function
from pyspark.sql.functions import lag,sum,count
window_spec = Window.partitionBy("user_id").orderBy("month")
df_prev_value = df.withColumn("prev_value", lag("value").over(window_spec))
df_prev_value.display()

# COMMAND ----------

# DBTITLE 1,flagging rows for increasing sugar levels
df_flagged = df_prev_value.withColumn("is_increasing", (col("value") > col("prev_value")).cast("int"))
df_flagged.display()

# COMMAND ----------

# DBTITLE 1,filtering user_id whose levels didn't increase
df_not_increased = df_flagged.filter(col("prev_value").isNotNull() & (col("value") <= col("prev_value"))).select("user_id").distinct()
df_not_increased.display()

# COMMAND ----------

# DBTITLE 1,excluding non increased user from main df
df_final  = df_flagged.select("user_id").distinct().subtract(df_not_increased)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 6

# COMMAND ----------

# 6. Calculate Length of Stay and Total Number of Tests During Hospitalization

# For each patient, calculate their hospital length of stay (in days) and the total number of lab tests done during their stay.

# | patient_id | name    | gender | dob        | admission_date | discharge_date |
# | ---------- | ------- | ------ | ---------- | -------------- | -------------- |
# | 1          | Alice   | F      | 1980-05-20 | 2023-07-01     | 2023-07-10     |
# | 2          | Bob     | M      | 1975-03-15 | 2023-07-02     | 2023-07-12     |
# | 3          | Charlie | M      | 1990-11-30 | 2023-07-05     | 2023-07-15     |

# | test_id | patient_id | test_type  | test_date  | test_result | normal_range_min | normal_range_max |
# | ------- | ---------- | ---------- | ---------- | ----------- | ---------------- | ---------------- |
# | 101     | 1          | Hemoglobin | 2023-07-02 | 13.5        | 12.0             | 16.0             |
# | 102     | 1          | Hemoglobin | 2023-07-08 | 11.0        | 12.0             | 16.0             |
# | 103     | 2          | Hemoglobin | 2023-07-03 | 14.0        | 12.0             | 16.0             |
# | 104     | 2          | Glucose    | 2023-07-04 | 180         | 70               | 110              |
# | 105     | 3          | Hemoglobin | 2023-07-06 | 15.0        | 12.0             | 16.0             |
# | 106     | 3          | Glucose    | 2023-07-10 | 95          | 70               | 110              |


# COMMAND ----------

# MAGIC %md
# MAGIC - Step 1: Create df using sample data
# MAGIC - Step 2: Calculate Length of Stay using **datediff** between discharge_date, admission_date
# MAGIC - Step 3: joined patients_df and tests_df on patient_id
# MAGIC - Step 4: Filtered tests that happened during the hospital stay
# MAGIC - Step 5: **grouped** the filtered data by patient_id, name, and length_of_stay, and used count(*) to get the total number of tests per patient.

# COMMAND ----------

# DBTITLE 1,creating df using given data
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import to_date,datediff

patient_data = [
    (1, "Alice", "F", "1980-05-20", "2023-07-01", "2023-07-10"),
    (2, "Bob", "M", "1975-03-15", "2023-07-02", "2023-07-12"),
    (3, "Charlie", "M", "1990-11-30", "2023-07-05", "2023-07-15")
]

patient_schema = StructType([
    StructField("patient_id", IntegerType()),
    StructField("name", StringType()),
    StructField("gender", StringType()),
    StructField("dob", StringType()),
    StructField("admission_date", StringType()),
    StructField("discharge_date", StringType())
])

test_data = [
    (101, 1, "Hemoglobin", "2023-07-02", 13.5, 12.0, 16.0),
    (102, 1, "Hemoglobin", "2023-07-08", 11.0, 12.0, 16.0),
    (103, 2, "Hemoglobin", "2023-07-03", 14.0, 12.0, 16.0),
    (104, 2, "Glucose", "2023-07-04", 180.0, 70.0, 110.0),
    (105, 3, "Hemoglobin", "2023-07-06", 15.0, 12.0, 16.0),
    (106, 3, "Glucose", "2023-07-10", 95.0, 70.0, 110.0)
]

test_schema = StructType([
    StructField("test_id", IntegerType()),
    StructField("patient_id", IntegerType()),
    StructField("test_type", StringType()),
    StructField("test_date", StringType()),
    StructField("test_result", DoubleType()),
    StructField("normal_range_min", DoubleType()),
    StructField("normal_range_max", DoubleType())
])


patients_df = spark.createDataFrame(patient_data, schema=patient_schema)
tests_df = spark.createDataFrame(test_data, schema=test_schema)


patients_df = patients_df.withColumn("admission_date", to_date("admission_date")).withColumn("discharge_date", to_date("discharge_date"))
tests_df = tests_df.withColumn("test_date", to_date("test_date"))

patients_df.display()
tests_df.display()

# COMMAND ----------

# DBTITLE 1,calculating length_of_stay by subtracting dates
patients_df = patients_df.withColumn("length_of_stay", datediff("discharge_date", "admission_date"))
patients_df.display()

# COMMAND ----------

# DBTITLE 1,joiing patient and test table using patient_id
joined_df = patients_df.join(tests_df, "patient_id").filter(col("test_date").between(col("admission_date"), col("discharge_date")))
joined_df.display()

# COMMAND ----------

# DBTITLE 1,calculating total_tests per user
df_final = joined_df.groupBy("patient_id","name","length_of_stay").agg(count("*").alias("total_tests"))
df_final.display()