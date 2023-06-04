# Databricks notebook source
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

#load races.csv into spark dataframe and infer schema
drivers_df = spark.read \
.option("header", True) \
.option("inferSchema", True) \
.json("/mnt/formula1datalake133/raw/drivers.json")

display(drivers_df)

drivers_df.printSchema()

#transformation
drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

#drop unwanted column
drivers_final_df = drivers_with_columns_df.drop(col("url"))

#finally export the dataframe to Parquet file to Processed folder
drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1datalake133/processed/drivers")
display(spark.read.parquet("/mnt/formula1datalake133/processed/drivers"))