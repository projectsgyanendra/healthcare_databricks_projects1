# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists healthcare_catalog
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog healthcare_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists healthcare_catalog.bronze;
# MAGIC create schema if not exists healthcare_catalog.silver;
# MAGIC create schema if not exists healthcare_catalog.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases from healthcare_catalog;

# COMMAND ----------

