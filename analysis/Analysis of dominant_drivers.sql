-- Databricks notebook source
Use formula1_db;
select * from race_results;

-- COMMAND ----------

select driver_name, race_year, position, points from race_results where position=1;

-- COMMAND ----------

select driver_name, race_year, position, points from race_results where (race_year between 2010 and 2020) and position=1;

-- COMMAND ----------

select driver_name, sum(points) as total_points, count(1) as total_races
from race_results
group by driver_name
order by total_points desc
limit 10

-- COMMAND ----------

