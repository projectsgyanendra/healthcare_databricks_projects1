# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare_catalog.analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN healthcare_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 1 — Total Patients..
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_patients
# MAGIC FROM healthcare_catalog.gold.dim_patient;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --KPI 2 — Total Encounters
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_encounters
# MAGIC FROM healthcare_catalog.gold.fact_encounter;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 3 — Encounters by Type
# MAGIC SELECT
# MAGIC     encounter_class,
# MAGIC     COUNT(*) AS total_encounters
# MAGIC FROM healthcare_catalog.gold.fact_encounter
# MAGIC GROUP BY encounter_class
# MAGIC ORDER BY total_encounters DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 4 — Total Healthcare Cost
# MAGIC SELECT
# MAGIC     ROUND(SUM(total_claim_cost), 2) AS total_healthcare_cost
# MAGIC FROM healthcare_catalog.gold.fact_encounter;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- KPI 5 — Average Encounter Cost
# MAGIC SELECT
# MAGIC     ROUND(AVG(total_claim_cost), 2) AS avg_encounter_cost
# MAGIC FROM healthcare_catalog.gold.fact_encounter;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 6 — Healthcare Cost by Encounter Type
# MAGIC SELECT
# MAGIC     encounter_class,
# MAGIC     COUNT(*) AS encounters,
# MAGIC     ROUND(SUM(total_claim_cost), 2) AS total_cost,
# MAGIC     ROUND(AVG(total_claim_cost), 2) AS average_cost
# MAGIC FROM healthcare_catalog.gold.fact_encounter
# MAGIC GROUP BY encounter_class
# MAGIC ORDER BY total_cost DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 7 — Top Conditions
# MAGIC SELECT
# MAGIC     condition_description,
# MAGIC     COUNT(*) AS condition_count
# MAGIC FROM healthcare_catalog.gold.fact_condition
# MAGIC GROUP BY condition_description
# MAGIC ORDER BY condition_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 8 — Top Medications
# MAGIC SELECT
# MAGIC     medication_description,
# MAGIC     COUNT(*) AS prescription_count
# MAGIC FROM healthcare_catalog.gold.fact_medication
# MAGIC GROUP BY medication_description
# MAGIC ORDER BY prescription_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 9 — Top Procedures
# MAGIC SELECT
# MAGIC     procedure_description,
# MAGIC     COUNT(*) AS procedure_count
# MAGIC FROM healthcare_catalog.gold.fact_procedure
# MAGIC GROUP BY procedure_description
# MAGIC ORDER BY procedure_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 10 — Patient Healthcare Expenses
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     healthcare_expenses
# MAGIC FROM healthcare_catalog.gold.dim_patient
# MAGIC ORDER BY healthcare_expenses DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- KPI 11 — Patient Encounter Count
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     COUNT(*) AS encounter_count
# MAGIC FROM healthcare_catalog.gold.fact_encounter
# MAGIC GROUP BY patient_id
# MAGIC ORDER BY encounter_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 12 — Monthly Encounters
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     COUNT(e.encounter_id) AS total_encounters
# MAGIC FROM healthcare_catalog.gold.fact_encounter e
# MAGIC
# MAGIC JOIN healthcare_catalog.gold.dim_date d
# MAGIC     ON CAST(date_format(e.start_datetime, 'yyyyMMdd') AS INT)
# MAGIC        = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.year,
# MAGIC     d.month;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 13 — Encounters by Year
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     COUNT(e.encounter_id) AS total_encounters
# MAGIC FROM healthcare_catalog.gold.fact_encounter e
# MAGIC
# MAGIC JOIN healthcare_catalog.gold.dim_date d
# MAGIC     ON CAST(date_format(e.start_datetime, 'yyyyMMdd') AS INT)
# MAGIC        = d.date_key
# MAGIC
# MAGIC GROUP BY d.year
# MAGIC ORDER BY d.year;

# COMMAND ----------

# MAGIC %sql
# MAGIC --KPI 14 — Patient Demographics
# MAGIC --gender...
# MAGIC SELECT
# MAGIC     gender,
# MAGIC     COUNT(*) AS patient_count
# MAGIC FROM healthcare_catalog.gold.dim_patient
# MAGIC GROUP BY gender
# MAGIC ORDER BY patient_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Race
# MAGIC SELECT
# MAGIC     race,
# MAGIC     COUNT(*) AS patient_count
# MAGIC FROM healthcare_catalog.gold.dim_patient
# MAGIC GROUP BY race
# MAGIC ORDER BY patient_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Ethnicity
# MAGIC SELECT
# MAGIC     ethnicity,
# MAGIC     COUNT(*) AS patient_count
# MAGIC FROM healthcare_catalog.gold.dim_patient
# MAGIC GROUP BY ethnicity
# MAGIC ORDER BY patient_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create an Analytics KPI Table
# MAGIC CREATE OR REPLACE TABLE healthcare_catalog.analytics.healthcare_kpis
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     'Total Patients' AS kpi_name,
# MAGIC     CAST(COUNT(*) AS DOUBLE) AS kpi_value
# MAGIC FROM healthcare_catalog.gold.dim_patient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Total Encounters',
# MAGIC     CAST(COUNT(*) AS DOUBLE)
# MAGIC FROM healthcare_catalog.gold.fact_encounter
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Total Conditions',
# MAGIC     CAST(COUNT(*) AS DOUBLE)
# MAGIC FROM healthcare_catalog.gold.fact_condition
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Total Medications',
# MAGIC     CAST(COUNT(*) AS DOUBLE)
# MAGIC FROM healthcare_catalog.gold.fact_medication
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Total Procedures',
# MAGIC     CAST(COUNT(*) AS DOUBLE)
# MAGIC FROM healthcare_catalog.gold.fact_procedure
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Total Immunizations',
# MAGIC     CAST(COUNT(*) AS DOUBLE)
# MAGIC FROM healthcare_catalog.gold.fact_immunization
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Total Healthcare Cost',
# MAGIC     ROUND(
# MAGIC         COALESCE(SUM(total_claim_cost), 0),
# MAGIC         2
# MAGIC     )
# MAGIC FROM healthcare_catalog.gold.fact_encounter;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.analytics.healthcare_kpis;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create Monthly Encounter Analytics
# MAGIC CREATE OR REPLACE TABLE healthcare_catalog.analytics.monthly_encounters
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     COUNT(e.encounter_id) AS total_encounters,
# MAGIC     ROUND(
# MAGIC         SUM(e.total_claim_cost),
# MAGIC         2
# MAGIC     ) AS total_cost,
# MAGIC     ROUND(
# MAGIC         AVG(e.total_claim_cost),
# MAGIC         2
# MAGIC     ) AS average_cost
# MAGIC
# MAGIC FROM healthcare_catalog.gold.fact_encounter e
# MAGIC
# MAGIC JOIN healthcare_catalog.gold.dim_date d
# MAGIC     ON CAST(
# MAGIC         date_format(e.start_datetime, 'yyyyMMdd')
# MAGIC         AS INT
# MAGIC     ) = d.date_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.analytics.monthly_encounters
# MAGIC ORDER BY year, month;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create Condition Analytics
# MAGIC CREATE OR REPLACE TABLE healthcare_catalog.analytics.top_conditions
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     condition_code,
# MAGIC     condition_description,
# MAGIC     COUNT(*) AS condition_count
# MAGIC
# MAGIC FROM healthcare_catalog.gold.fact_condition
# MAGIC
# MAGIC GROUP BY
# MAGIC     condition_code,
# MAGIC     condition_description
# MAGIC
# MAGIC ORDER BY condition_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create Medication Analytics
# MAGIC CREATE OR REPLACE TABLE healthcare_catalog.analytics.top_medications
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     medication_code,
# MAGIC     medication_description,
# MAGIC     COUNT(*) AS medication_count,
# MAGIC     ROUND(SUM(total_cost), 2) AS total_medication_cost
# MAGIC
# MAGIC FROM healthcare_catalog.gold.fact_medication
# MAGIC
# MAGIC GROUP BY
# MAGIC     medication_code,
# MAGIC     medication_description
# MAGIC
# MAGIC ORDER BY medication_count DESC;