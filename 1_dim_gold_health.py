# Databricks notebook source
from pyspark.sql.functions import (
    col,
    row_number,
    monotonically_increasing_id
)
from pyspark.sql.window import Window

# COMMAND ----------

patients_silver = spark.table(
    "healthcare_catalog.silver.slv_patients"
)

# COMMAND ----------

#Create surrogate key:...
window_spec = Window.orderBy("patient_id")

dim_patient = patients_silver \
    .withColumn(
        "patient_key",
        row_number().over(window_spec)
    ) \
    .select(
        "patient_key",
        "patient_id",
        "first_name",
        "middle_name",
        "last_name",
        "birth_date",
        "death_date",
        "marital_status",
        "race",
        "ethnicity",
        "gender",
        "city",
        "state",
        "county",
        "zip_code",
        "latitude",
        "longitude",
        "healthcare_expenses",
        "healthcare_coverage",
        "income"
    )

# COMMAND ----------

dim_patient.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.dim_patient"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.gold.dim_patient
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md ##Create dim_date

# COMMAND ----------

from pyspark.sql.functions import (
    sequence,
    to_date,
    explode,
    year,
    quarter,
    month,
    weekofyear,
    dayofmonth,
    dayofweek,
    date_format
)

# COMMAND ----------

#generate dates ..
date_df = spark.sql("""
SELECT explode(
    sequence(
        to_date('2010-01-01'),
        to_date('2030-12-31'),
        interval 1 day
    )
) AS full_date
""")

# COMMAND ----------

dim_date = date_df \
    .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int")) \
    .withColumn("year", year("full_date")) \
    .withColumn("quarter", quarter("full_date")) \
    .withColumn("month", month("full_date")) \
    .withColumn("month_name", date_format("full_date", "MMMM")) \
    .withColumn("week_of_year", weekofyear("full_date")) \
    .withColumn("day", dayofmonth("full_date")) \
    .withColumn("day_of_week", dayofweek("full_date")) \
    .withColumn("day_name", date_format("full_date", "EEEE"))

# COMMAND ----------

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.dim_date"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.gold.dim_date
# MAGIC LIMIT 10;

# COMMAND ----------

#Create fact_encounter...
encounters_silver = spark.table(
    "healthcare_catalog.silver.slv_encounters"
)

dim_patient = spark.table(
    "healthcare_catalog.gold.dim_patient"
).select(
    "patient_key",
    "patient_id"
)  

# COMMAND ----------

encounters_silver = spark.table(
    "healthcare_catalog.silver.slv_encounters"
)

dim_patient = spark.table(
    "healthcare_catalog.gold.dim_patient"
).select(
    "patient_key",
    "patient_id"
)

# COMMAND ----------

fact_encounter = encounters_silver.join(
    dim_patient,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

fact_encounter = fact_encounter.select(
    "encounter_id",
    "patient_key",
    "patient_id",
    "start_datetime",
    "end_datetime",
    "organization_id",
    "provider_id",
    "payer_id",
    "encounter_class",
    "encounter_code",
    "encounter_description",
    "base_encounter_cost",
    "total_claim_cost",
    "payer_coverage",
    "reason_code",
    "reason_description"
)

# COMMAND ----------

fact_encounter.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.fact_encounter"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.gold.fact_encounter
# MAGIC LIMIT 10;

# COMMAND ----------

#Create fact_condition...
conditions_silver = spark.table(
    "healthcare_catalog.silver.slv_conditions"
)

# COMMAND ----------

#Join patient key:...
fact_condition = conditions_silver.join(
    dim_patient,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

fact_condition = fact_condition.select(
    "patient_id",
    "patient_key",
    "encounter_id",
    "condition_start_date",
    "condition_end_date",
    "condition_code",
    "condition_description"
)

# COMMAND ----------

fact_condition.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.fact_condition"
    )

# COMMAND ----------

#Create fact_medication...
medications_silver = spark.table(
    "healthcare_catalog.silver.slv_medications"
)

fact_medication = medications_silver.join(
    dim_patient,
    on="patient_id",
    how="inner"
) 

# COMMAND ----------

fact_medication = fact_medication.select(
    "patient_id",
    "patient_key",
    "encounter_id",
    "payer_id",
    "medication_start_datetime",
    "medication_end_datetime",
    "medication_code",
    "medication_description",
    "base_cost",
    "payer_coverage",
    "dispenses",
    "total_cost",
    "reason_code",
    "reason_description"
)

# COMMAND ----------

fact_medication.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.fact_medication"
    )

# COMMAND ----------

# MAGIC
# MAGIC %md #Create fact_observation

# COMMAND ----------

observations_silver = spark.table(
    "healthcare_catalog.silver.slv_observations"
)

fact_observation = observations_silver.join(
    dim_patient,
    on="patient_id",
    how="inner"
) 

# COMMAND ----------

fact_observation = fact_observation.select(
    "patient_id",
    "patient_key",
    "encounter_id",
    "observation_datetime",
    "category",
    "observation_code",
    "observation_description",
    "observation_value",
    "units",
    "value_type"
) 

# COMMAND ----------

fact_observation.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.fact_observation"
    ) 

# COMMAND ----------

# MAGIC %md #Create fact_procedure

# COMMAND ----------

procedures_silver = spark.table(
    "healthcare_catalog.silver.slv_procedures"
)

fact_procedure = procedures_silver.join(
    dim_patient,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

fact_procedure = fact_procedure.select(
    "patient_id",
    "patient_key",
    "encounter_id",
    "procedure_start_datetime",
    "procedure_end_datetime",
    "procedure_code",
    "procedure_description",
    "base_cost",
    "reason_code",
    "reason_description"
)

# COMMAND ----------

fact_procedure.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.fact_procedure"
    )

# COMMAND ----------

# MAGIC %md #Create fact_immunization

# COMMAND ----------

immunizations_silver = spark.table(
    "healthcare_catalog.silver.slv_immunizations"
)

fact_immunization = immunizations_silver.join(
    dim_patient,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

fact_immunization = fact_immunization.select(
    "patient_id",
    "patient_key",
    "encounter_id",
    "immunization_datetime",
    "immunization_code",
    "immunization_description",
    "base_cost"
)

# COMMAND ----------

fact_immunization.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.gold.fact_immunization"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN healthcare_catalog.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_patient' AS table_name,
# MAGIC        COUNT(*) AS records
# MAGIC FROM healthcare_catalog.gold.dim_patient
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_date',
# MAGIC        COUNT(*)
# MAGIC FROM healthcare_catalog.gold.dim_date
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_encounter',
# MAGIC        COUNT(*)
# MAGIC FROM healthcare_catalog.gold.fact_encounter
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_condition',
# MAGIC        COUNT(*)
# MAGIC FROM healthcare_catalog.gold.fact_condition
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_medication',
# MAGIC        COUNT(*)
# MAGIC FROM healthcare_catalog.gold.fact_medication
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_observation',
# MAGIC        COUNT(*)
# MAGIC FROM healthcare_catalog.gold.fact_observation
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_procedure',
# MAGIC        COUNT(*)
# MAGIC FROM healthcare_catalog.gold.fact_procedure
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_immunization',
# MAGIC        COUNT(*)
# MAGIC FROM healthcare_catalog.gold.fact_immunization;