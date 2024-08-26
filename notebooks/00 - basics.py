# Databricks notebook source
# MAGIC %md
# MAGIC #### Create a storage credential and external location
# MAGIC
# MAGIC ##### Storage credential 
# MAGIC
# MAGIC 1. Login to Azure Portal and search for the Databricks Access Connector
# MAGIC 2. Copy the resource Id from the connector overview page
# MAGIC 3. Create a new storage credential via UI:
# MAGIC    - Set `name` to your username
# MAGIC    - Set `connectorId` to the resource Id copied from Azure portal
# MAGIC
# MAGIC
# MAGIC ##### External location
# MAGIC
# MAGIC 1. Login to Azure portal and find your storage account
# MAGIC 2. Find a container inside the storage account
# MAGIC 3. Create a new external location via UI:
# MAGIC   - Set `name` to your username
# MAGIC   - Select the storage credential created previously.
# MAGIC   - Configure your external location path in a following way:
# MAGIC
# MAGIC **`abfss://{YOUR_CONTAINER_NAME}@{YOUR_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/`**
# MAGIC
# MAGIC
# MAGIC Afterwards, please copy this path into the variables below.
# MAGIC

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set the base location path 
# MAGIC
# MAGIC Please set it exactly to the path below:
# MAGIC
# MAGIC **`abfss://{YOUR_CONTAINER_NAME}@{YOUR_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/`**

# COMMAND ----------


base_location_path = ... # abfss://{YOUR_CONTAINER_NAME}@{YOUR_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create catalog

# COMMAND ----------


command = """
CREATE CATALOG {catalog_name}
MANAGED LOCATION '{base_location_path}/_managed'
""".format(catalog_name=catalog_name, base_location_path=base_location_path)

print(command)
spark.sql(command)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set a catalog to your user

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select current_catalog()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS default.department
# MAGIC (deptcode INT, deptname STRING, location STRING);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC INSERT INTO default.department VALUES
# MAGIC (10,"FINANCE","EDINBURGH"),
# MAGIC (20,"SOFTWARE","PADDINGTON"),
# MAGIC (30,"SALES","MAIDSTONE"),
# MAGIC (40,"MARKETING","DARLINGTON"),
# MAGIC (50,"ADMIN","BIRMINGHAM");

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create a UC External table

# COMMAND ----------


spark.sql("""
CREATE OR REPLACE TABLE default.nyctaxi_trips_ext
  LOCATION "{base_location_path}/external/tables/nyc"
  AS SELECT * FROM samples.nyctaxi.trips
""".format(base_location_path=base_location_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ###UC Volumes
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ####**Create** a Managed Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS default.arbitrary_files;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Upload a sample csv file (iris.csv) from internet

# COMMAND ----------

import requests
from pathlib import Path 

source_url = "https://gist.githubusercontent.com/curran/a08a1080b88344b0c8a7/raw/0e7a9b0a5d22642a06d3d5b9bcbad9890c8ee534/iris.csv"
response = requests.get(source_url)
output_path = Path("/Volumes") / catalog_name / "default/arbitrary_files/iris.csv"

output_path.write_text(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Do some analytics on top of this file

# COMMAND ----------

iris_df = spark.read.csv(f"/Volumes/{catalog_name}/sample/arbitrary_files/iris.csv")
display(iris_df)

display(
  iris_df.summary()
)
