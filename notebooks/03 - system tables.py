# Databricks notebook source
# MAGIC %md
# MAGIC ####Table access events of a specific user

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM system.access.audit
# MAGIC WHERE user_identity["email"] = "<USER EMAIL>"
# MAGIC AND service_name = "unityCatalog"
# MAGIC AND action_name IN ('createTable', 'commandSubmit','getTable','deleteTable')
# MAGIC AND datediff(now(), event_time) < 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Refined query for table access events of a specific user

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC         action_name as `EVENT`,
# MAGIC         event_time as `WHEN`,
# MAGIC         IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`,
# MAGIC         IFNULL(request_params.commandText,'GET table') AS `QUERY TEXT`
# MAGIC FROM system.access.audit
# MAGIC WHERE user_identity.email = '<USER EMAIL>'
# MAGIC         AND action_name IN ('createTable','commandSubmit','getTable','deleteTable')
# MAGIC         AND datediff(now(), event_date) < 10
# MAGIC         ORDER BY event_date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Query ACCOUNT_LEVEL level events of specific user

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM system.access.audit
# MAGIC WHERE service_name = "unityCatalog"
# MAGIC AND audit_level = "ACCOUNT_LEVEL"
# MAGIC AND user_identity.email = '<USER EMAIL>'
# MAGIC AND datediff(now(), event_time) < 10;

# COMMAND ----------


