# Databricks notebook source
#read the parquet files from processed folder and change the column names to make them clearer in the context of joining different tables
raw_folder_path = "/mnt/formula1datalake133/raw"
processed_folder_path = "/mnt/formula1datalake133/processed"
presentation_folder_path = "/mnt/formula1datalake133/presentation"

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

from pyspark.sql.functions import col
from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------
final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")
# COMMAND ----------

