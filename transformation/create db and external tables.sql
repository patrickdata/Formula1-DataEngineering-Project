-- Databricks notebook source
-- MAGIC %sql
-- MAGIC create database if not exists formula1_db;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC presentation_folder_path = "/mnt/formula1datalake133/presentation"
-- MAGIC
-- MAGIC raceresults_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC
-- MAGIC raceresults_df.write.mode("overwrite").format("parquet").saveAsTable("formula1_db.race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC presentation_folder_path = "/mnt/formula1datalake133/presentation"
-- MAGIC
-- MAGIC raceresults_df = spark.read.parquet(f"{presentation_folder_path}/driver_standings")
-- MAGIC
-- MAGIC raceresults_df.write.mode("overwrite").format("parquet").saveAsTable("formula1_db.driver_standings")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC presentation_folder_path = "/mnt/formula1datalake133/presentation"
-- MAGIC
-- MAGIC raceresults_df = spark.read.parquet(f"{presentation_folder_path}/constructor_standings")
-- MAGIC
-- MAGIC raceresults_df.write.mode("overwrite").format("parquet").saveAsTable("formula1_db.constructor_standings")

-- COMMAND ----------

desc extended formula1_db.race_results

-- COMMAND ----------

show tables in formula1_db;

-- COMMAND ----------

drop table formula1_db.race_results

-- COMMAND ----------

create table formula1_db.race_results
as 
select * from 