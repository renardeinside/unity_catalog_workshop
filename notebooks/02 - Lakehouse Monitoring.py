# Databricks notebook source
# MAGIC %md
# MAGIC ###DB Demos
# MAGIC
# MAGIC [Demo source](https://www.databricks.com/resources/demos/tutorials/data-warehouse-and-bi/monitor-your-data-quality-with-lakehouse-monitoring?itm_data=demo_center)

# COMMAND ----------

!pip install dbdemos

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dbdemos

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('lakehouse-monitoring', catalog={YOUR_CATALOG}, schema='lakehouse_monitoring')
