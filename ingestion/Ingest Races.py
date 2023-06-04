# Databricks notebook source
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

#load races.csv into spark dataframe and infer schema
races_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.csv("/mnt/formula1datalake133/raw/races.csv")

display(races_df)

races_df.printSchema()

#drop column URL
races_selected_df = races_df.drop("url")

#rename required columns
races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitid", "circuit_id") \

#combine date and time columns to create new column race_timestamp
races_combined_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
races_combined_df = races_combined_df.drop("date")
races_combined_df = races_combined_df.drop("time")

#add the ingestion_date column to dataframe for audit purpose
races_final_df = races_combined_df.withColumn("ingestion_date", current_timestamp()) 

#finally export the dataframe to Parquet file to Processed folder
races_final_df.write.mode("overwrite").parquet("/mnt/formula1datalake133/processed/races")
display(spark.read.parquet("/mnt/formula1datalake133/processed/races"))