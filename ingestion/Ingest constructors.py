# Databricks notebook source
#load instructors.json into spark dataframe and infer schema
constructors_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.json("/mnt/formula1datalake133/raw/constructors.json")

display(constructors_df)

constructors_df.printSchema()

#extract only required columns, drop the url column
from pyspark.sql.functions import col
constructors_selected_df = constructors_df.drop("url")
#or you can simply drop the last columns from the circuits_df

#rename required columns
constructors_renamed_df = constructors_selected_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref")

#add the ingestion_date column to dataframe for audit purpose
from pyspark.sql.functions import current_timestamp
constructors_final_df = constructors_renamed_df.withColumn("ingestion_date", current_timestamp()) 

#finally export the dataframe to Parquet file to Processed folder
constructors_final_df.write.mode("overwrite").parquet("/mnt/formula1datalake133/processed/constructors")
display(spark.read.parquet("/mnt/formula1datalake133/processed/constructors"))