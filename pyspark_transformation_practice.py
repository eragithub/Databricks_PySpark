# Databricks notebook source
df=spark.read.table("samples.bakehouse.sales_franchises")

# COMMAND ----------

#df.show()
display(df)

# COMMAND ----------

df.write.saveAsTable("test_catalog.test_schema.sales_franchises")

# COMMAND ----------

df1 = spark.read.table("test_catalog.test_schema.sales_franchises")

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df1.limit(10))

# COMMAND ----------

df2 = df1.limit(10)

print(df2.count())
df2.count() 
#print(df2.printSchema)

# COMMAND ----------

# Either of the below works 

#df_csv = spark.read.csv("/Volumes/test_catalog/test_schema/test_volume/sales.csv")
df_csv = spark.read.format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales.csv")
display(df_csv)

# COMMAND ----------

df_csv = spark.read\
    .option("header", "true")\
    .option("inferschema", "true")\
    .format("csv")\
    .load("/Volumes/test_catalog/test_schema/test_volume/sales.csv")

display(df_csv)

# COMMAND ----------


#when there is no header available for the received file, we use schema() method while reading..

schema_str = ("id integer,customer_name string,product string,quantity integer, price integer,remarks string")
df3 = spark.read\
    .schema(schema_str)\
    .csv("/Volumes/test_catalog/test_schema/test_volume/sales_no_header.csv")
display(df3)

# COMMAND ----------

#This is a modular way building the schema when there is no header. 

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

schema_str1 = StructType([StructField("id", IntegerType(), True),
                          StructField("customer_name", StringType(), True),
                          StructField("product", StringType(), True),
                          StructField("qty", IntegerType(), True),
                          StructField("price", DoubleType(), True),
                          StructField("remark", StringType(), True)])

df4 = spark.read\
    .schema(schema_str1)\
    .format("csv")\
    .load("/Volumes/test_catalog/test_schema/test_volume/sales_no_header.csv")

display(df4.limit(2))

# COMMAND ----------

#when the received csv has different delimiter than comma,we then have to handle it via option method while reading as below. 

df5 = spark.read\
        .option("inferSchema", True)\
        .option("header", True)\
        .option("delimiter", "|")\
        .format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales_pipe.csv")
display(df5)

# COMMAND ----------

df6 = spark.read\
        .option("inferSchema", True)\
        .option("header", True)\
        .option("delimiter", "|")\
        .option("quote", "'")\
        .format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales_pipe_single_quote.csv")
display(df6)

# COMMAND ----------

df7 = spark.read\
        .option("inferSchema", True)\
        .option("header", True)\
        .option("delimiter", "|")\
        .option("quote", "'")\
        .option("escape", "#")\
        .format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales_pipe_single_quote_escape.csv")
display(df7)



# COMMAND ----------


df7 = spark.read\
        .option("inferSchema", True)\
        .option("header", True)\
        .option("delimiter", "|")\
        .option("quote", "'")\
        .option("escape", "#")\
        .option("multiline", True)\
        .format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales_pipe_single_quote_escape_multiline.csv")
display(df7)


# COMMAND ----------

df_json1 = spark.read.format("json").load("/Volumes/test_catalog/test_schema/test_volume/sales_single_line.json")
display(df_json1)