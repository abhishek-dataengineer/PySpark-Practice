# Databricks notebook source
# MAGIC %md
# MAGIC ### CSV Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/')

# COMMAND ----------

df_bigMart = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load("/FileStore/BigMart_Sales.csv")

# COMMAND ----------

df_bigMart.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/')

# COMMAND ----------

df_json = spark.read.format('json')\
                .option("inferSchema", True)\
                .option("header", True)\
                .option("multiline", False)\
                .load("/FileStore/tables/drivers.json")

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Schema Definition

# COMMAND ----------

df_bigMart.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier String,
                    Item_Weight String,
                    Item_Fat_Content String,
                    Item_Visibility double,
                    Item_Type String,
                    Item_MRP double,
                    Outlet_Identifier String,
                    Outlet_Establishment_Year integer,
                    Outlet_Size String,
                    Outlet_Location_Type String,
                    Outlet_Type String,
                    Item_Outlet_Sales double
                '''


df_mart = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(my_ddl_schema)\
                    .load("/FileStore/tables/BigMart_Sales.csv")

df_mart.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

structSchema = StructType (
    [
        StructField("Item_Identifer", StringType(), True),
        StructField("Item_Weight", StringType(), True),
        StructField("Item_Fat_Content", StringType(), True),
        StructField("Item_Visibility", StringType(), True),
        StructField("Item_Type", StringType(), True),
        StructField("Item_MRP", StringType(), True),
        StructField("Outlet_Identifier", StringType(), True),
        StructField("Outlet_Establishment_Year", StringType(), True),
        StructField("Outlet_Size", StringType(), True),
        StructField("Outlet_Location_Type", StringType(), True),
        StructField("Outlet_Type", StringType(), True),
        StructField("Item_Outlet_Sales", StringType(), True)
    ]
)



df_Mart = spark.read.format("csv")\
                    .option("header", True)\
                    .schema(structSchema)\
                    .load("/FileStore/tables/BigMart_Sales.csv")

df_Mart.printSchema()

# COMMAND ----------

df_Mart.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select

# COMMAND ----------

df_Mart.select("Item_Identifer", "Item_Weight", "Item_Fat_Content").display()

# COMMAND ----------

df_Mart.select(col("Item_Identifer"), col("Item_Weight"), col("Item_Fat_Content")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

df_Mart.select(col("Item_Identifer").alias("Item_id")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter / Where
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1: Filter the data with Fat_Content is Regular

# COMMAND ----------

df_Mart.filter(col("Item_Fat_Content")=="Regular").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2: Slice the data with Item_Type is SoftDrinks and their weight should be less than 10.

# COMMAND ----------

df_Mart.filter((col("Item_Type")=='Soft Drinks') & (col("Item_Weight") < 10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-3: Fetch the data with Tier (Tier1, Tier2) and Outlet Size is null

# COMMAND ----------

df_Mart.filter((col("Outlet_Size").isNull()) & (col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df_Mart.withColumnRenamed("Item_Weight", "Item_Wt").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

df = df_Mart.withColumn("flag", lit("new"))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df = df.withColumn("multiply", round(col("Item_Weight") * col("Item_MRP"),2))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df = df.withColumn("Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Regular", "Reg"))\
        .withColumn("Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Low Fat", "LF"))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TypeCasting

# COMMAND ----------

df = df.withColumn("Item_Weight", col("Item_Weight").cast(DoubleType()))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Sort**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df = df.sort(col("Item_Weight").desc())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df = df.sort(col("Item_Visibility").asc())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-3

# COMMAND ----------

df = df.sort(["Item_Visibility", "Item_Weight"], ascending = [0,0])

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-4

# COMMAND ----------

df = df.sort(["Item_Visibility", "Item_Weight"], ascending = [0,1])

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

#df.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df = df.drop("Item_Visibility")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df = df.drop("flag", "multiply")

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DropDuplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df.drop_duplicates(subset=["Item_Type"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initcap()

# COMMAND ----------

df.select(initcap("Item_Type").alias("Item_Type")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upper()

# COMMAND ----------

df.select(upper("Item_Type").alias("Item_Type")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Date Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### CURRENT_DATE

# COMMAND ----------

df = df.withColumn("Curr_Date", current_date())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE_ADD

# COMMAND ----------

df = df.withColumn("Week_After", date_add("curr_date", 7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE_SUB

# COMMAND ----------

df = df.withColumn("Week_Before", date_sub("curr_date", 7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DateDIFF()

# COMMAND ----------

df = df.withColumn("DateDiff", datediff("Week_After", "Curr_Date"))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date_Format()

# COMMAND ----------

df = df.withColumn("Week_Before", date_format("week_Before", "dd-mm-yyyy"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping Nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset = ['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filling Nulls

# COMMAND ----------

df.fillna("Not Available").display()

# COMMAND ----------

df.fillna("Not Available",subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Split and **Indexing**

# COMMAND ----------

# MAGIC %md
# MAGIC ### split()

# COMMAND ----------

df.withColumn("Outlet_Type", split("Outlet_Type", " ")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing()

# COMMAND ----------

#df.withColumn("Outlet_Type", split("Outlet_Type", " ")[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Explode**

# COMMAND ----------

df_exp = df.withColumn("Outlet_Type", split("Outlet_Type", " ")).display()

# COMMAND ----------

df.withColumn("Outlet_Type", explode(split("Outlet_Type", " "))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **GroupBy**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df.groupBy("Item_Type").agg(sum("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df.groupBy("Item_Type").agg(avg("Item_MRP").alias("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-3

# COMMAND ----------

df.groupBy("Item_Type", "Outlet_Size").agg(sum("Item_MRP").alias("Total_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-4

# COMMAND ----------

df.groupBy("Item_Type", "Outlet_Size").agg(sum("Item_MRP").alias("Total_MRP"), avg("Item_MRP").alias("Avg_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Pivot**

# COMMAND ----------

df.groupBy("Item_Type").pivot("Outlet_Size").agg(avg("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # When-**Otherwise**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df_flag = df.withColumn("Veg_flag", when(col("Item_Type")=="Meat", "Non-Veg").otherwise("Veg"))
df_flag.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-2

# COMMAND ----------

df_flag.withColumn(
    "Veg_exp_flag",
    when((col("Veg_flag") == "Veg") & (col("Item_MRP") < 100), "Veg_Inexpensive")
    .when((col("Veg_flag") == "Veg") & (col("Item_MRP") > 100), "Veg_Expensive")
    .otherwise("Non_Veg")
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Collect_List**

# COMMAND ----------

data = [
    ("user1", "book1"),
    ("user1", "book2"),
    ("user2", "book2"),
    ("user2", "book4"),
    ("user3", "book1")
]

schema = "user string, book string"

df_book = spark.createDataFrame(data, schema)

df_book.display()

# COMMAND ----------

df_book.groupBy("user").agg(collect_list("book")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Union Vs UnionByName

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Preparing

# COMMAND ----------

data1 = [
    ('1', 'Kad'),
    ('2', 'Sid')
]

Schema1 = "id string, name string"

df1 = spark.createDataFrame(data1, Schema1)




# COMMAND ----------

data2 = [
    ('3', 'Rahul'),
    ('4', 'Jas')
]

schema2 = "id string, name string"

df2 = spark.createDataFrame(data2, schema2)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying Union

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying UnionByName

# COMMAND ----------

data3 = [
    ('Kad', "1"),
    ('Sid', "2")
]

Schema3 = "name string, id string"

df3 = spark.createDataFrame(data3, Schema3)

# COMMAND ----------

df3.union(df2).display()

# COMMAND ----------

df3.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Joins**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join

# COMMAND ----------

dataj1 = [
    ('1', 'gaur', 'd01'),
    ('2', 'kit', 'd02'),
    ('3', 'sam', 'd03'),
    ('4', 'tim', 'd03'),
    ('5', 'aman', 'd05'),
    ('6', 'nad', 'd06')
]

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING'

df1 = spark.createDataFrame(dataj1, schema=schemaj1)



# Second dataset
dataj2 = [
    ('d01', 'HR'),
    ('d02', 'Marketing'),
    ('d03', 'Accounts'),
    ('d04', 'IT'),
    ('d05', 'Finance')
]

schemaj2 = 'dept_id STRING, dept_name STRING'

df2 = spark.createDataFrame(dataj2, schema=schemaj2)

# Show both dataframes
df1.show()
df2.show()

# COMMAND ----------

df1.join(df2, df1["dept_id"]==df2["dept_id"], "inner").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join

# COMMAND ----------

df1.join(df2, df1["dept_id"]==df2["dept_id"], "left").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join

# COMMAND ----------

df1.join(df2, df1["dept_id"]==df2["dept_id"], "right").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Join

# COMMAND ----------

df1.join(df2, df1["dept_id"]==df2["dept_id"], "full").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join

# COMMAND ----------

df1.join(df2, df1["dept_id"]==df2["dept_id"], "anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Window **Functions**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row_Number()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn("row_col", row_number().over(Window.orderBy("Item_Identifer"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### rank()

# COMMAND ----------

df.withColumn("ranking", rank().over(Window.orderBy("Item_Identifer"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dense_rank()

# COMMAND ----------

df.withColumn("dense_rank", dense_rank().over(Window.orderBy("Item_Identifer"))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario: Cumulative Sum

# COMMAND ----------

df.withColumn("cum_sum", sum("Item_MRP").over(Window.orderBy("Item_Type").rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()
              
              

# COMMAND ----------

df.withColumn("Total_Sum", sum("Item_MRP").over(Window.orderBy("Item_Type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # User Defined **Functions (UDF)**

# COMMAND ----------

#step-1

def my_func(x):
    return x*x

# COMMAND ----------

#step-2

my_udf = udf(my_func)

# COMMAND ----------

#df.withColumn("myNewCol", my_udf("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data **Writing**

# COMMAND ----------

# MAGIC %md
# MAGIC ### csv

# COMMAND ----------

df.write.format("csv")\
        .mode("overwrite")\
        .save("/FileStore/tables/Csv/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### parquet

# COMMAND ----------

df.write.format("parquet")\
        .mode("overwrite")\
        .save("/FileStore/tables/Parquet/data.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### delta

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .save("/FileStore/tables/Delta/data.delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ### check all files

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Managed Table Vs External **Table**

# COMMAND ----------

# MAGIC %md
# MAGIC Managed Table: It is managed by databricks, so if we delete table or schema so data will be deleted automatically. But we can rollback it within 24-48hrs, after that not.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC External table: It is stored externally somewhere like data lake, and managed by us. So if we delete schema or table in databricks then still it'll be present in externally.

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### CreateTempView

# COMMAND ----------

df.createTempView("my_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * 
# MAGIC from my_view
# MAGIC where Item_Fat_Content = 'LF'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'LF'")

df_sql.display()

# COMMAND ----------

