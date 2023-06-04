# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using service principal
# MAGIC 1. Register Azure AD Application/SErvice Principal
# MAGIC 2. Generate a secret for the application
# MAGIC 3. Set Spark Config with App/Client ID, Directory/Tenant ID & Secret
# MAGIC 4. Assign Role "Storage Blob Data Contributor" to the Data Lake

# COMMAND ----------

client_id = "0c1ee390-ac39-4cd9-914c-11e52783023f"
tenant_id = "d1f8fe1c-a958-443c-83e0-47cf6411bb41"
client_secret = "V~38Q~0PaUQdr9O5_ftu-Ewt84xbqoJfmJinNcR8"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1datalake133.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1datalake133.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1datalake133.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1datalake133.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1datalake133.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1datalake133.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://raw@formula1datalake133.dfs.core.windows.net/circuits.csv"))