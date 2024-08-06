-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create a storage credential and external location
-- MAGIC
-- MAGIC ##### Storage credential 
-- MAGIC
-- MAGIC 1. Login to Azure Portal and choose the Databricks Access Connector
-- MAGIC 2. Copy the connector Id from the connector overview page
-- MAGIC 3. Create a new storage credential via UI
-- MAGIC
-- MAGIC
-- MAGIC ##### External location
-- MAGIC
-- MAGIC 1. Login to Azure portal and find your storage account
-- MAGIC 2. Find a container inside the storage account
-- MAGIC 3. Create a new external location via UI. Use the storage credential created previously as a storage credential. 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create a catalog and a schema
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS {user_name}
MANAGED LOCATION 'adls://<container>@<storage account name>.net/__managed'


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create a UC managed table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS {catalog_name}.default.department
(deptcode INT, deptname STRING, location STRING);

-- COMMAND ----------

INSERT INTO {catalog_name}.default.department VALUES
(10,"FINANCE","EDINBURGH"),
(20,"SOFTWARE","PADDINGTON"),
(30,"SALES","MAIDSTONE"),
(40,"MARKETING","DARLINGTON"),
(50,"ADMIN","BIRMINGHAM");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create a UC External table

-- COMMAND ----------

CREATE OR REPLACE TABLE {catalog_name}.default.nyctaxi_trips_ext
  LOCATION "<your-location>/external/tables/nyc"
  AS SELECT * FROM samples.nyctaxi.trips;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###UC Volumes
-- MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/volumes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####**Create** a Managed Volume

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS {catalog_name}.default.arbitrary_files;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Upload a sample csv file (iris.csv) from internet

-- COMMAND ----------

-- MAGIC %sh curl https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv --output /Volumes/{catalog_name}/default/arbitrary_files/iris.csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Do some analytics on top of this file

-- COMMAND ----------

-- MAGIC %python
-- MAGIC iris_df = spark.read.csv(f"/Volumes/{catalog_name}/sample/arbitrary_files/iris.csv")
-- MAGIC display(iris_df)
-- MAGIC
-- MAGIC display(
-- MAGIC   iris_df.summary()
-- MAGIC )
