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

# This reads a single line records in a json file.  note: json file can have records by single line or in multi/hierarchy. when hierarchical format then we must tell the spak while reading that this file contains rows in multiple lines. 

df_json1 = spark.read.format("json").load("/Volumes/test_catalog/test_schema/test_volume/sales_single_line.json")
display(df_json1)

df_json2 = spark.read\
    .option("multiline", True)\
    .format("json").load("/Volumes/test_catalog/test_schema/test_volume/sales_multi_line.json")
display(df_json2)

# COMMAND ----------

# Lets now wite the processed file into any type to the required location. In this case json, that can be changed to csv or any other type that we need the output in..  

df_json2.write.format("json").save("/Volumes/test_catalog/test_schema/test_volume/savedfile")


# COMMAND ----------

# As understood about the benefits of Parquet file because of its architectural designed to have this file in a columnar storage data file format. so let me save the output of the processed json file into a Parquet format to see what why the Parquet format file is not for meant for human readable.

df_json2.write.format("parquet").save("/Volumes/test_catalog/test_schema/test_volume/saved_parquet_file")


# COMMAND ----------

# lets read the parquet format data file for processing. Imp: you can notice the time taken for reading the parquet file and json time differences. 

df_parqt = spark.read.format("Parquet").load("/Volumes/test_catalog/test_schema/test_volume/saved_parquet_file")

display(df_parqt)


# COMMAND ----------

# lets now work to handle the corrupted files .. while reading itself we have several options to specify to understand the file corruption..
# generally there are two modes of options which work differently based on the requirement .. and the first mode has three options and while the second mode has one option. we will examine all of the modes from both the modes. 

df_corrupt_json = spark.read\
        .option("inferSchema", True)\
        .option("header", True)\
        .format("json").load("/Volumes/test_catalog/test_schema/test_volume/sales_corrupted.json")
display(df_corrupt_json)
# note: for json by default gives a column for storing the corrupted records which is _corrupt_record.. you can notice it below in the output. 

# lets see how it will display the corrupted csv file. 

schema_str = ("id integer,customer_name string,product string,quantity integer, price integer,remarks string, _corrupt_record string")

df_corrupt_csv = spark.read\
         .schema(schema_str)\
            .format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales_corrupted.csv")
display(df_corrupt_csv) # you notice that there wont be a _corrupt_record column is created by default to store the corrupted records. so for csv, we have to specify manually the required column in the schema_str. 




# COMMAND ----------

# lets now apply the read modes for handling the corrupted files as we have seen the records being writed to the corrupted column in the above practice. we can now decide whether to hold those corrupted records or not ..
# PLS NTOE: THIS option("mode", "PERMISSIVE") IS A DEFAULT BEHAVIOUR.. 
# DROPMALFORMED read mode is omitting the corrupted records from the display. 
# FailFast read mode it wont parse the file but fails to notify. 

# mode option 2 : This one is badrecordspath. This will send the corrupted to silently to the specified location for our analysis later.when we use this option mode, we must specify the location to send the bad records.  


df_corrupt_json = spark.read\
        .option("inferSchema", True)\
        .option("header", True)\
        .option("mode", "Failfast")\
        .format("json").load("/Volumes/test_catalog/test_schema/test_volume/sales_corrupted.json")
display(df_corrupt_json)
# note: for json by default gives a column for storing the corrupted records which is _corrupt_record.. you can notice it below in the output. 

# lets see how it will display the corrupted csv file. 

schema_str = ("id integer,customer_name string,product string,quantity integer, price integer,remarks string, _corrupt_record string")

df_corrupt_csv = spark.read\
         .schema(schema_str)\
             .option("mode", "Failfast")\
            .format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales_corrupted.csv")
display(df_corrupt_csv) # you notice that there wont be a _corrupt_record column is created by default to store the corrupted records. so for csv, we have to specify manually the required column in the schema_str. 

# COMMAND ----------

# so , lets try to send the corrupted records to the specified location by the option mode 2. 

df_corrupt_json = spark.read\
        .option("inferSchema", True)\
        .option("header", True)\
        .option("badrecordspath", "/Volumes/test_catalog/test_schema/test_volume/")\
        .format("json").load("/Volumes/test_catalog/test_schema/test_volume/sales_corrupted.json")
display(df_corrupt_json)
# note: for json by default gives a column for storing the corrupted records which is _corrupt_record.. you can notice it below in the output. 

# lets see how it will display the corrupted csv file. 

schema_str = ("id integer,customer_name string,product string,quantity integer, price integer,remarks string, _corrupt_record string")

df_corrupt_csv = spark.read\
         .schema(schema_str)\
             .option("badrecordspath", "/Volumes/test_catalog/test_schema/test_volume/")\
            .format("csv").load("/Volumes/test_catalog/test_schema/test_volume/sales_corrupted.csv")
display(df_corrupt_csv) # you notice that there wont be a _corrupt_record column is created by default to store the corrupted records. so for csv, we have to specify manually the required column in the schema_str. 

# COMMAND ----------

#lets read the corrupted records from the location .. # pls note the corrupted records are stored in the json format. 

df_rd_corrupted_records = spark.read.format("json").option("recursivefilelookup", True).load("/Volumes/test_catalog/test_schema/test_volume/20261018T133101/bad_records")
display(df_rd_corrupted_records)