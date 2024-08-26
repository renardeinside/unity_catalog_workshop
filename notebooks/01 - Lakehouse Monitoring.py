# Databricks notebook source
# MAGIC %md
# MAGIC ###DB Demos
# MAGIC
# MAGIC [Demo source](https://www.databricks.com/resources/demos/tutorials/data-warehouse-and-bi/monitor-your-data-quality-with-lakehouse-monitoring?itm_data=demo_center)

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

!pip install dbdemos

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dbdemos

# COMMAND ----------

dbdemos.install('lakehouse-monitoring', catalog=catalog_name, schema='lakehouse_monitoring', serverless=True)
