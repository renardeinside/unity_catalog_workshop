# Databricks notebook source
# MAGIC %md
# MAGIC ###DB Demos
# MAGIC https://github.com/databricks-demos/dbdemos

# COMMAND ----------

!pip install dbdemos

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dbdemos

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('lakehouse-fsi-smart-claims', catalog='uc_workshop', schema='sample')
