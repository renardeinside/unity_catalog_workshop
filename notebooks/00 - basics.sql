-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Create a catalog and a schema

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS uc_workshop;
CREATE SCHEMA IF NOT EXISTS uc_workshop.sample;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create a UC managed table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS uc_workshop.sample.department
(deptcode INT, deptname STRING, location STRING);

-- COMMAND ----------

INSERT INTO uc_workshop.sample.department VALUES
(10,"FINANCE","EDINBURGH"),
(20,"SOFTWARE","PADDINGTON"),
(30,"SALES","MAIDSTONE"),
(40,"MARKETING","DARLINGTON"),
(50,"ADMIN","BIRMINGHAM");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create a UC External table

-- COMMAND ----------

CREATE OR REPLACE TABLE uc_workshop.sample.nyctaxi_trips
  LOCATION 'abfss://data@ks02ucdemone.dfs.core.windows.net/external/tables'
  AS SELECT * FROM samples.nyctaxi.trips;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###UC Volumes
-- MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/volumes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####**Create** a Managed Volume

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS uc_workshop.sample.arbitrary_files;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Upload a sample csv file (iris.csv) from internet

-- COMMAND ----------

-- MAGIC %sh curl https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv --output /Volumes/uc_workshop/sample/arbitrary_files/iris.csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC iris_df = spark.read.csv("/Volumes/uc_workshop/sample/arbitrary_files/iris.csv")
-- MAGIC display(iris_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####**Create** an External Volume

-- COMMAND ----------

CREATE EXTERNAL VOLUME uc_workshop.sample.arbitrary_files_external
  LOCATION 'abfss://data@ks02ucdemone.dfs.core.windows.net/external/volumes'

-- COMMAND ----------


