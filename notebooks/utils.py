# Databricks notebook source
user_name = spark.sql("select current_user()").toPandas().iloc[0,0]
catalog_name = user_name.split("@")[0].replace(".", "_")

# COMMAND ----------

print(f"user_name: {user_name}")
print(f"catalog_name: {catalog_name}")
