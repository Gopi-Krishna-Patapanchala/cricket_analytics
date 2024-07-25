# Databricks notebook source
dbutils.widgets.text("Access_key","")
dbutils.widgets.text("Secret_key","")
dbutils.widgets.text("folder_details","cricket-data-analytics/")
dbutils.widgets.text("mount_point_name","")

# COMMAND ----------

access_key = dbutils.widgets.get("Access_key")
secret_key = dbutils.widgets.get("Secret_key")
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = dbutils.widgets.get("folder_details")
mount_name = dbutils.widgets.get("mount_point_name")

# COMMAND ----------

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

dbutils.fs.ls(f"/mnt/{mount_name}")

# COMMAND ----------


