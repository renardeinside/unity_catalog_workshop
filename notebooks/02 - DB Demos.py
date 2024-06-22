# Databricks notebook source
!pip install dbdemos

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dbdemos

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('lakehouse-fsi-smart-claims', catalog='uc_workshop', schema='sample')
