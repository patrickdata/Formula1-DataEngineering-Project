# Databricks notebook source
raw_folder_path = "/mnt/formula1datalake133/raw"
processed_folder_path = "/mnt/formula1datalake133/processed"
presentation_folder_path = "/mnt/formula1datalake133/presentation"

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

#aggregate the points and wins
from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# display(driver_standings_df.filter("race_year = 2020"))

display(driver_standings_df)

#rank the drivers based on total points and wins
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

display(final_df.filter("race_year = 2020"))

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")