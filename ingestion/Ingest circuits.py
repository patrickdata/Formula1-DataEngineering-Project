# Databricks notebook source
#load circuits.csv into spark dataframe and infer schema
circuits_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv("/mnt/formula1datalake133/raw/circuits.csv")

display(circuits_df)

circuits_df.printSchema()

#extract only required columns, drop the url column
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
#or you can simply drop the last columns from the circuits_df

#rename required columns
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

#add the ingestion_date column to dataframe for audit purpose
from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

#finally export the dataframe to Parquet file to Processed folder
circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1datalake133/processed/circuits")
display(spark.read.parquet("/mnt/formula1datalake133/processed/circuits"))