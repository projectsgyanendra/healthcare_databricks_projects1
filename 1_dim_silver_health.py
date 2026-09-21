# Databricks notebook source
#Synthea CSV
    
# Bronze
    
#Clean
    
#Validate
    
#Deduplicate
    
#Standardize
    
# Silver Delta Tables


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN healthcare_catalog;

# COMMAND ----------

#Import libraries
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    regexp_replace,
    to_date,
    when,
    lit,
    current_timestamp
)

# COMMAND ----------

#Read Bronze Patients
patients_bronze = spark.table(
    "healthcare_catalog.bronze.brz_patients"
)

display(patients_bronze)

# COMMAND ----------

#Select required columns
patients_silver = patients_bronze.select(
    "Id",
    "BIRTHDATE",
    "DEATHDATE",
    "FIRST",
    "MIDDLE",
    "LAST",
    "MARITAL",
    "RACE",
    "ETHNICITY",
    "GENDER",
    "CITY",
    "STATE",
    "COUNTY",
    "ZIP",
    "LAT",
    "LON",
    "HEALTHCARE_EXPENSES",
    "HEALTHCARE_COVERAGE",
    "INCOME"
)

# COMMAND ----------


#Rename columns

patients_silver = patients_silver \
    .withColumnRenamed("Id", "patient_id") \
    .withColumnRenamed("BIRTHDATE", "birth_date") \
    .withColumnRenamed("DEATHDATE", "death_date") \
    .withColumnRenamed("FIRST", "first_name") \
    .withColumnRenamed("MIDDLE", "middle_name") \
    .withColumnRenamed("LAST", "last_name") \
    .withColumnRenamed("MARITAL", "marital_status") \
    .withColumnRenamed("RACE", "race") \
    .withColumnRenamed("ETHNICITY", "ethnicity") \
    .withColumnRenamed("GENDER", "gender") \
    .withColumnRenamed("CITY", "city") \
    .withColumnRenamed("STATE", "state") \
    .withColumnRenamed("COUNTY", "county") \
    .withColumnRenamed("ZIP", "zip_code") \
    .withColumnRenamed("LAT", "latitude") \
    .withColumnRenamed("LON", "longitude") \
    .withColumnRenamed(
        "HEALTHCARE_EXPENSES",
        "healthcare_expenses"
    ) \
    .withColumnRenamed(
        "HEALTHCARE_COVERAGE",
        "healthcare_coverage"
    )

# COMMAND ----------

#Standardize text
#gender
patients_silver = patients_silver.withColumn(
    "gender",
    upper(trim(col("gender")))
)
#race
patients_silver = patients_silver.withColumn(
    "race",
    trim(col("race"))
)
#state
patients_silver = patients_silver.withColumn(
    "state",
    upper(trim(col("state")))
)

# COMMAND ----------

#Remove invalid patient IDs
patients_silver = patients_silver.filter(
    col("patient_id").isNotNull()
)

# COMMAND ----------

# remove blank IDs:
patients_silver = patients_silver.filter(
    trim(col("patient_id")) != ""
)

# COMMAND ----------

#Remove duplicate patients
patients_silver = patients_silver.dropDuplicates(
    ["patient_id"]
)

# COMMAND ----------

display(patients_silver)


# COMMAND ----------

#Add Silver metadata
patients_silver = patients_silver.withColumn(
    "silver_processed_timestamp",
    current_timestamp()
)

# COMMAND ----------

patients_silver = patients_silver.join(
    patients_bronze.select(
        "Id",
        "_batch_id",
        "_source_file",
        "_ingest_timestamp"
    ),
    patients_silver.patient_id ==
    patients_bronze.Id,
    "left"
).drop("Id")

# COMMAND ----------

patients_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_patients"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.silver.slv_patients
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md #Check data quality

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_patients,
# MAGIC     COUNT(patient_id) AS patients_with_id
# MAGIC FROM healthcare_catalog.silver.slv_patients;

# COMMAND ----------

# MAGIC %md #Check duplicate IDs:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     COUNT(*) AS cnt
# MAGIC FROM healthcare_catalog.silver.slv_patients
# MAGIC GROUP BY patient_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md #Silver Encounters
# MAGIC

# COMMAND ----------

# MAGIC %md ##read bronze

# COMMAND ----------

encounters_bronze = spark.table(
    "healthcare_catalog.bronze.brz_encounters"
)

# COMMAND ----------

encounters_silver = encounters_bronze.select(
    "Id",
    "START",
    "STOP",
    "PATIENT",
    "ORGANIZATION",
    "PROVIDER",
    "PAYER",
    "ENCOUNTERCLASS",
    "CODE",
    "DESCRIPTION",
    "BASE_ENCOUNTER_COST",
    "TOTAL_CLAIM_COST",
    "PAYER_COVERAGE",
    "REASONCODE",
    "REASONDESCRIPTION"
)

# COMMAND ----------

#Rename:
encounters_silver = encounters_silver \
    .withColumnRenamed("Id", "encounter_id") \
    .withColumnRenamed("START", "start_datetime") \
    .withColumnRenamed("STOP", "end_datetime") \
    .withColumnRenamed("PATIENT", "patient_id") \
    .withColumnRenamed(
        "ORGANIZATION",
        "organization_id"
    ) \
    .withColumnRenamed(
        "PROVIDER",
        "provider_id"
    ) \
    .withColumnRenamed(
        "PAYER",
        "payer_id"
    ) \
    .withColumnRenamed(
        "ENCOUNTERCLASS",
        "encounter_class"
    ) \
    .withColumnRenamed("CODE", "encounter_code") \
    .withColumnRenamed(
        "DESCRIPTION",
        "encounter_description"
    ) \
    .withColumnRenamed(
        "BASE_ENCOUNTER_COST",
        "base_encounter_cost"
    ) \
    .withColumnRenamed(
        "TOTAL_CLAIM_COST",
        "total_claim_cost"
    ) \
    .withColumnRenamed(
        "PAYER_COVERAGE",
        "payer_coverage"
    ) \
    .withColumnRenamed(
        "REASONCODE",
        "reason_code"
    ) \
    .withColumnRenamed(
        "REASONDESCRIPTION",
        "reason_description"
    )

# COMMAND ----------

#Remove records without an encounter ID:
encounters_silver = encounters_silver.filter(
    col("encounter_id").isNotNull()
)

# COMMAND ----------

#Remove records without patient ID:
encounters_silver = encounters_silver.filter(
    col("patient_id").isNotNull()
)

# COMMAND ----------

#Remove duplicates:
encounters_silver = encounters_silver.dropDuplicates(
    ["encounter_id"]
)

# COMMAND ----------

#Standardize encounter class:
encounters_silver = encounters_silver.withColumn(
    "encounter_class",
    lower(trim(col("encounter_class")))
)

# COMMAND ----------

#Check patient relationship:
valid_patients = spark.table(
    "healthcare_catalog.silver.slv_patients"
).select("patient_id")

# COMMAND ----------

#Find encounters that have valid patients:
encounters_silver = encounters_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
) 

# COMMAND ----------

display(encounters_silver)

# COMMAND ----------

#Write Silver Encounters
encounters_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_encounters"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.silver.slv_encounters
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Verify the healthcare relationship
# MAGIC SELECT
# MAGIC     p.patient_id,
# MAGIC     p.first_name,
# MAGIC     p.last_name,
# MAGIC     e.encounter_id,
# MAGIC     e.encounter_class,
# MAGIC     e.start_datetime,
# MAGIC     e.encounter_description
# MAGIC FROM healthcare_catalog.silver.slv_patients p
# MAGIC INNER JOIN healthcare_catalog.silver.slv_encounters e
# MAGIC     ON p.patient_id = e.patient_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md #Silver Conditions

# COMMAND ----------

conditions_bronze = spark.table("healthcare_catalog.bronze.brz_conditions")

# COMMAND ----------

conditions_silver = conditions_bronze.select(
    "START",
    "STOP",
    "PATIENT",
    "ENCOUNTER",
    "CODE",
    "DESCRIPTION"
)

# COMMAND ----------

conditions_silver = conditions_silver \
    .withColumnRenamed("START", "condition_start_date") \
    .withColumnRenamed("STOP", "condition_end_date") \
    .withColumnRenamed("PATIENT", "patient_id") \
    .withColumnRenamed("ENCOUNTER", "encounter_id") \
    .withColumnRenamed("CODE", "condition_code") \
    .withColumnRenamed("DESCRIPTION", "condition_description")

# COMMAND ----------

#Validate Patient ID cannot be null:
conditions_silver = conditions_silver.filter(
    col("patient_id").isNotNull()
) 

# COMMAND ----------

#Condition code cannot be null:
conditions_silver = conditions_silver.filter(
    col("condition_code").isNotNull()
)


# COMMAND ----------

#Clean description:
conditions_silver = conditions_silver.withColumn(
    "condition_description",
    trim(col("condition_description"))
) 

# COMMAND ----------

#Validate patient relationship
valid_patients = spark.table(
    "healthcare_catalog.silver.slv_patients"
).select("patient_id")

conditions_silver = conditions_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

#Write Silver
conditions_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_conditions"
    )
    

# COMMAND ----------

# MAGIC %md #Silver Medications

# COMMAND ----------

medications_bronze = spark.table(
    "healthcare_catalog.bronze.brz_medications"
)

# COMMAND ----------

medications_silver = medications_bronze.select(
    "START",
    "STOP",
    "PATIENT",
    "PAYER",
    "ENCOUNTER",
    "CODE",
    "DESCRIPTION",
    "BASE_COST",
    "PAYER_COVERAGE",
    "DISPENSES",
    "TOTALCOST",
    "REASONCODE",
    "REASONDESCRIPTION"
)

# COMMAND ----------

#rename
medications_silver = medications_silver \
    .withColumnRenamed("START", "medication_start_datetime") \
    .withColumnRenamed("STOP", "medication_end_datetime") \
    .withColumnRenamed("PATIENT", "patient_id") \
    .withColumnRenamed("PAYER", "payer_id") \
    .withColumnRenamed("ENCOUNTER", "encounter_id") \
    .withColumnRenamed("CODE", "medication_code") \
    .withColumnRenamed("DESCRIPTION", "medication_description") \
    .withColumnRenamed("BASE_COST", "base_cost") \
    .withColumnRenamed("PAYER_COVERAGE", "payer_coverage") \
    .withColumnRenamed("DISPENSES", "dispenses") \
    .withColumnRenamed("TOTALCOST", "total_cost") \
    .withColumnRenamed("REASONCODE", "reason_code") \
    .withColumnRenamed(
        "REASONDESCRIPTION",
        "reason_description"
    )

# COMMAND ----------

#validate
medications_silver = medications_silver.filter(
    col("patient_id").isNotNull()
)

# COMMAND ----------

medications_silver = medications_silver.filter(
    col("medication_code").isNotNull()
)

# COMMAND ----------

#clean text 
medications_silver = medications_silver.withColumn(
    "medication_description",
    trim(col("medication_description"))
)

# COMMAND ----------

#Validate patients
medications_silver = medications_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
)


# COMMAND ----------

medications_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_medications"
    )
    

# COMMAND ----------

# MAGIC %md #Silver Observations

# COMMAND ----------

observations_bronze = spark.table(
    "healthcare_catalog.bronze.brz_observations"
)

# COMMAND ----------

observations_silver = observations_bronze.select(
    "DATE",
    "PATIENT",
    "ENCOUNTER",
    "CATEGORY",
    "CODE",
    "DESCRIPTION",
    "VALUE",
    "UNITS",
    "TYPE"
)

# COMMAND ----------

observations_silver = observations_silver \
    .withColumnRenamed(
        "DATE",
        "observation_datetime"
    ) \
    .withColumnRenamed(
        "PATIENT",
        "patient_id"
    ) \
    .withColumnRenamed(
        "ENCOUNTER",
        "encounter_id"
    ) \
    .withColumnRenamed(
        "CATEGORY",
        "category"
    ) \
    .withColumnRenamed(
        "CODE",
        "observation_code"
    ) \
    .withColumnRenamed(
        "DESCRIPTION",
        "observation_description"
    ) \
    .withColumnRenamed(
        "VALUE",
        "observation_value"
    ) \
    .withColumnRenamed(
        "UNITS",
        "units"
    ) \
    .withColumnRenamed(
        "TYPE",
        "value_type"
    )

# COMMAND ----------

#Clean text
observations_silver = observations_silver \
    .withColumn(
        "category",
        trim(col("category"))
    ) \
    .withColumn(
        "observation_description",
        trim(col("observation_description"))
    ) \
    .withColumn(
        "units",
        trim(col("units"))
    )

# COMMAND ----------

#Validate patient
observations_silver = observations_silver.filter(
    col("patient_id").isNotNull()
)


# COMMAND ----------

observations_silver = observations_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

observations_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_observations"
    )

# COMMAND ----------

# MAGIC %md #Silver Procedures

# COMMAND ----------

procedures_bronze = spark.table(
    "healthcare_catalog.bronze.brz_procedures"
)

# COMMAND ----------

procedures_silver = procedures_bronze.select(
    "START",
    "STOP",
    "PATIENT",
    "ENCOUNTER",
    "CODE",
    "DESCRIPTION",
    "BASE_COST",
    "REASONCODE",
    "REASONDESCRIPTION"
)

# COMMAND ----------

procedures_silver = procedures_silver \
    .withColumnRenamed(
        "START",
        "procedure_start_datetime"
    ) \
    .withColumnRenamed(
        "STOP",
        "procedure_end_datetime"
    ) \
    .withColumnRenamed(
        "PATIENT",
        "patient_id"
    ) \
    .withColumnRenamed(
        "ENCOUNTER",
        "encounter_id"
    ) \
    .withColumnRenamed(
        "CODE",
        "procedure_code"
    ) \
    .withColumnRenamed(
        "DESCRIPTION",
        "procedure_description"
    ) \
    .withColumnRenamed(
        "BASE_COST",
        "base_cost"
    ) \
    .withColumnRenamed(
        "REASONCODE",
        "reason_code"
    ) \
    .withColumnRenamed(
        "REASONDESCRIPTION",
        "reason_description"
    )

# COMMAND ----------

#Validate
procedures_silver = procedures_silver.filter(
    col("patient_id").isNotNull()
)

# COMMAND ----------

procedures_silver = procedures_silver.filter(
    col("procedure_code").isNotNull()
)

# COMMAND ----------

procedures_silver = procedures_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

procedures_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_procedures"
    )

# COMMAND ----------

# MAGIC %md #Silver Allergies

# COMMAND ----------

allergies_bronze = spark.table(
    "healthcare_catalog.bronze.brz_allergies"
)

# COMMAND ----------

allergies_silver = allergies_bronze.select(
    "START",
    "STOP",
    "PATIENT",
    "ENCOUNTER",
    "CODE",
    "DESCRIPTION"
)

# COMMAND ----------

allergies_silver = allergies_silver \
    .withColumnRenamed(
        "START",
        "allergy_start_date"
    ) \
    .withColumnRenamed(
        "STOP",
        "allergy_end_date"
    ) \
    .withColumnRenamed(
        "PATIENT",
        "patient_id"
    ) \
    .withColumnRenamed(
        "ENCOUNTER",
        "encounter_id"
    ) \
    .withColumnRenamed(
        "CODE",
        "allergy_code"
    ) \
    .withColumnRenamed(
        "DESCRIPTION",
        "allergy_description"
    )

# COMMAND ----------

#validate
allergies_silver = allergies_silver.filter(
    col("patient_id").isNotNull()
)

# COMMAND ----------

allergies_silver = allergies_silver.filter(
    col("allergy_code").isNotNull()
)

# COMMAND ----------

#clean
allergies_silver = allergies_silver.withColumn(
    "allergy_description",
    trim(col("allergy_description"))
)

# COMMAND ----------

#validate patients..
allergies_silver = allergies_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

allergies_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_allergies"
    )

# COMMAND ----------

# MAGIC %md #Silver Immunizations

# COMMAND ----------

immunizations_bronze = spark.table(
    "healthcare_catalog.bronze.brz_immunizations"
)

# COMMAND ----------

immunizations_silver = immunizations_bronze.select(
    "DATE",
    "PATIENT",
    "ENCOUNTER",
    "CODE",
    "DESCRIPTION",
    "BASE_COST"
) 

# COMMAND ----------

immunizations_silver = immunizations_silver \
    .withColumnRenamed(
        "DATE",
        "immunization_datetime"
    ) \
    .withColumnRenamed(
        "PATIENT",
        "patient_id"
    ) \
    .withColumnRenamed(
        "ENCOUNTER",
        "encounter_id"
    ) \
    .withColumnRenamed(
        "CODE",
        "immunization_code"
    ) \
    .withColumnRenamed(
        "DESCRIPTION",
        "immunization_description"
    ) \
    .withColumnRenamed(
        "BASE_COST",
        "base_cost"
    )

# COMMAND ----------

#validate...
immunizations_silver = immunizations_silver.filter(
    col("patient_id").isNotNull()
)

# COMMAND ----------

immunizations_silver = immunizations_silver.filter(
    col("immunization_code").isNotNull()
)

# COMMAND ----------

#validate patients..
immunizations_silver = immunizations_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

immunizations_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_immunizations"
    )

# COMMAND ----------

# MAGIC %md #Silver Careplans

# COMMAND ----------

careplans_bronze = spark.table(
    "healthcare_catalog.bronze.brz_careplans"
)

# COMMAND ----------

careplans_silver = careplans_bronze.select(
    "Id",
    "START",
    "STOP",
    "PATIENT",
    "ENCOUNTER",
    "CODE",
    "DESCRIPTION",
    "REASONCODE",
    "REASONDESCRIPTION"
)

# COMMAND ----------

careplans_silver = careplans_silver \
    .withColumnRenamed(
        "Id",
        "careplan_id"
    ) \
    .withColumnRenamed(
        "START",
        "careplan_start_date"
    ) \
    .withColumnRenamed(
        "STOP",
        "careplan_end_date"
    ) \
    .withColumnRenamed(
        "PATIENT",
        "patient_id"
    ) \
    .withColumnRenamed(
        "ENCOUNTER",
        "encounter_id"
    ) \
    .withColumnRenamed(
        "CODE",
        "careplan_code"
    ) \
    .withColumnRenamed(
        "DESCRIPTION",
        "careplan_description"
    ) \
    .withColumnRenamed(
        "REASONCODE",
        "reason_code"
    ) \
    .withColumnRenamed(
        "REASONDESCRIPTION",
        "reason_description"
    )

# COMMAND ----------

#validate...
careplans_silver = careplans_silver.filter(
    col("careplan_id").isNotNull()
)

# COMMAND ----------

careplans_silver = careplans_silver.filter(
    col("patient_id").isNotNull()
)

# COMMAND ----------

#Remove duplicate careplans:...
careplans_silver = careplans_silver.dropDuplicates(
    ["careplan_id"]
)

# COMMAND ----------

#Validate patient:...
careplans_silver = careplans_silver.join(
    valid_patients,
    on="patient_id",
    how="inner"
)

# COMMAND ----------

careplans_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "healthcare_catalog.silver.slv_careplans"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN healthcare_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Data Quality Checks..
# MAGIC --Patient duplicates...
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM healthcare_catalog.silver.slv_patients
# MAGIC GROUP BY patient_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Encounter_duplicates
# MAGIC SELECT
# MAGIC     encounter_id,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM healthcare_catalog.silver.slv_encounters
# MAGIC GROUP BY encounter_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Conditions without patients..
# MAGIC
# MAGIC SELECT COUNT(*) AS orphan_conditions
# MAGIC FROM healthcare_catalog.silver.slv_conditions c
# MAGIC LEFT JOIN healthcare_catalog.silver.slv_patients p
# MAGIC     ON c.patient_id = p.patient_id
# MAGIC WHERE p.patient_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Medications without patients
# MAGIC SELECT COUNT(*) AS orphan_medications
# MAGIC FROM healthcare_catalog.silver.slv_medications m
# MAGIC LEFT JOIN healthcare_catalog.silver.slv_patients p
# MAGIC     ON m.patient_id = p.patient_id
# MAGIC WHERE p.patient_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Observations without patients
# MAGIC SELECT COUNT(*) AS orphan_observations
# MAGIC FROM healthcare_catalog.silver.slv_observations o
# MAGIC LEFT JOIN healthcare_catalog.silver.slv_patients p
# MAGIC     ON o.patient_id = p.patient_id
# MAGIC WHERE p.patient_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Check encounter relationships
# MAGIC SELECT COUNT(*) AS orphan_encounters
# MAGIC FROM healthcare_catalog.silver.slv_encounters e
# MAGIC LEFT JOIN healthcare_catalog.silver.slv_patients p
# MAGIC     ON e.patient_id = p.patient_id
# MAGIC WHERE p.patient_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating a Silver data-quality summary...
# MAGIC
# MAGIC CREATE OR REPLACE TABLE healthcare_catalog.silver.silver_quality_summary
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     'patients' AS table_name,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(patient_id) AS valid_records,
# MAGIC     COUNT(*) - COUNT(patient_id) AS null_records
# MAGIC FROM healthcare_catalog.silver.slv_patients
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'encounters',
# MAGIC     COUNT(*),
# MAGIC     COUNT(encounter_id),
# MAGIC     COUNT(*) - COUNT(encounter_id)
# MAGIC FROM healthcare_catalog.silver.slv_encounters
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'conditions',
# MAGIC     COUNT(*),
# MAGIC     COUNT(condition_code),
# MAGIC     COUNT(*) - COUNT(condition_code)
# MAGIC FROM healthcare_catalog.silver.slv_conditions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'medications',
# MAGIC     COUNT(*),
# MAGIC     COUNT(medication_code),
# MAGIC     COUNT(*) - COUNT(medication_code)
# MAGIC FROM healthcare_catalog.silver.slv_medications
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'observations',
# MAGIC     COUNT(*),
# MAGIC     COUNT(observation_code),
# MAGIC     COUNT(*) - COUNT(observation_code)
# MAGIC FROM healthcare_catalog.silver.slv_observations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'procedures',
# MAGIC     COUNT(*),
# MAGIC     COUNT(procedure_code),
# MAGIC     COUNT(*) - COUNT(procedure_code)
# MAGIC FROM healthcare_catalog.silver.slv_procedures
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'allergies',
# MAGIC     COUNT(*),
# MAGIC     COUNT(allergy_code),
# MAGIC     COUNT(*) - COUNT(allergy_code)
# MAGIC FROM healthcare_catalog.silver.slv_allergies
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'immunizations',
# MAGIC     COUNT(*),
# MAGIC     COUNT(immunization_code),
# MAGIC     COUNT(*) - COUNT(immunization_code)
# MAGIC FROM healthcare_catalog.silver.slv_immunizations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'careplans',
# MAGIC     COUNT(*),
# MAGIC     COUNT(careplan_id),
# MAGIC     COUNT(*) - COUNT(careplan_id)
# MAGIC FROM healthcare_catalog.silver.slv_careplans;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.silver.silver_quality_summary;

# COMMAND ----------

