# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
    DoubleType,
    IntegerType
)

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    input_file_name,
    lit,
    sha2,
    concat_ws
)

# COMMAND ----------

batch_id = "batch_001"

# COMMAND ----------

patients_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("BIRTHDATE", DateType(), True),
    StructField("DEATHDATE", DateType(), True),
    StructField("SSN", StringType(), True),
    StructField("DRIVERS", StringType(), True),
    StructField("PASSPORT", StringType(), True),
    StructField("PREFIX", StringType(), True),
    StructField("FIRST", StringType(), True),
    StructField("MIDDLE", StringType(), True),
    StructField("LAST", StringType(), True),
    StructField("SUFFIX", StringType(), True),
    StructField("MAIDEN", StringType(), True),
    StructField("MARITAL", StringType(), True),
    StructField("RACE", StringType(), True),
    StructField("ETHNICITY", StringType(), True),
    StructField("GENDER", StringType(), True),
    StructField("BIRTHPLACE", StringType(), True),
    StructField("ADDRESS", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("COUNTY", StringType(), True),
    StructField("ZIP", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LON", DoubleType(), True),
    StructField("HEALTHCARE_EXPENSES", DoubleType(), True),
    StructField("HEALTHCARE_COVERAGE", DoubleType(), True),
    StructField("INCOME", IntegerType(), True)
])

# COMMAND ----------

patients_df = spark.read \
    .option("header", "true") \
    .schema(patients_schema) \
    .csv(
        "/Volumes/healthcare_catalog/source_data/raw/patients.csv"
    )

# COMMAND ----------

display(patients_df)

# COMMAND ----------

patients_df.printSchema()

# COMMAND ----------

#Add Bronze metadata
patients_bronze = patients_df \
    .withColumn(
        "_ingest_timestamp",
        current_timestamp()
    ) \
    .withColumn(
        "_ingest_date",
        current_date()
    ) \
    .withColumn(
        "_source_file",
        input_file_name()
    ) \
    .withColumn(
        "_batch_id",
        lit(batch_id)
    )

# COMMAND ----------

#Add a record hash--A record hash can help us identify whether the source record changed.
patients_bronze = patients_bronze.withColumn(
    "_record_hash",
    sha2(
        concat_ws(
            "||",
            *patients_df.columns
        ),
        256
    )
)

# COMMAND ----------

from pyspark.sql.functions import col

patients_bronze = patients_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *patients_df.columns), 256))

patients_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_patients"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM healthcare_catalog.bronze.brz_patients
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     Id,
# MAGIC     FIRST,
# MAGIC     LAST,
# MAGIC     _ingest_timestamp,
# MAGIC     _ingest_date,
# MAGIC     _source_file,
# MAGIC     _batch_id,
# MAGIC     _record_hash
# MAGIC FROM healthcare_catalog.bronze.brz_patients
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md 
# MAGIC #Encounters

# COMMAND ----------

encounters_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("START", TimestampType(), True),
    StructField("STOP", TimestampType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ORGANIZATION", StringType(), True),
    StructField("PROVIDER", StringType(), True),
    StructField("PAYER", StringType(), True),
    StructField("ENCOUNTERCLASS", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("BASE_ENCOUNTER_COST", DoubleType(), True),
    StructField("TOTAL_CLAIM_COST", DoubleType(), True),
    StructField("PAYER_COVERAGE", DoubleType(), True),
    StructField("REASONCODE", StringType(), True),
    StructField("REASONDESCRIPTION", StringType(), True)
])

# COMMAND ----------

encounters_df = spark.read \
    .option("header", "true") \
    .schema(encounters_schema) \
    .csv(
        "/Volumes/healthcare_catalog/source_data/raw/encounters.csv"
    )

# COMMAND ----------

#Add metadata:
encounters_bronze = encounters_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", input_file_name()) \
    .withColumn("_batch_id", lit(batch_id))

# COMMAND ----------

encounters_bronze = encounters_bronze.withColumn(
    "_record_hash",
    sha2(concat_ws("||", *encounters_df.columns),256
    )
)

# COMMAND ----------

from pyspark.sql.functions import col

encounters_bronze = encounters_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *encounters_df.columns), 256))

encounters_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_encounters"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #Conditions

# COMMAND ----------

conditions_schema = StructType([
    StructField("START", DateType(), True),
    StructField("STOP", DateType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True)
])

# COMMAND ----------

conditions_df = spark.read \
    .option("header", "true") \
    .schema(conditions_schema) \
    .csv(
        "/Volumes/healthcare_catalog/bronze/raw/conditions.csv"
    )

# COMMAND ----------

from pyspark.sql.functions import col

conditions_df = spark.read \
    .option("header", "true") \
    .schema(conditions_schema) \
    .csv("/Volumes/healthcare_catalog/source_data/raw/conditions.csv")

conditions_bronze = conditions_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *conditions_df.columns), 256))

# COMMAND ----------

conditions_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_conditions"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Medications

# COMMAND ----------

medications_schema = StructType([
    StructField("START", TimestampType(), True),
    StructField("STOP", TimestampType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("PAYER", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("BASE_COST", DoubleType(), True),
    StructField("PAYER_COVERAGE", DoubleType(), True),
    StructField("DISPENSES", DoubleType(), True),
    StructField("TOTALCOST", DoubleType(), True),
    StructField("REASONCODE", StringType(), True),
    StructField("REASONDESCRIPTION", StringType(), True)
])

# COMMAND ----------

medications_df = spark.read \
    .option("header", "true") \
    .schema(medications_schema) \
    .csv(
        "/Volumes/healthcare_catalog/bronze/raw/medications.csv"
    )

# COMMAND ----------

from pyspark.sql.functions import col

medications_df = spark.read \
    .option("header", "true") \
    .schema(medications_schema) \
    .csv("/Volumes/healthcare_catalog/source_data/raw/medications.csv")

medications_bronze = medications_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *medications_df.columns), 256))

# COMMAND ----------

medications_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_medications"
    )

# COMMAND ----------

# MAGIC %md ##Observations

# COMMAND ----------

observations_schema = StructType([
    StructField("DATE", TimestampType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CATEGORY", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("VALUE", StringType(), True),
    StructField("UNITS", StringType(), True),
    StructField("TYPE", StringType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import col

observations_df = spark.read \
    .option("header", "true") \
    .schema(observations_schema) \
    .csv("/Volumes/healthcare_catalog/source_data/raw/observations.csv")

observations_bronze = observations_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *observations_df.columns), 256))

# COMMAND ----------

observations_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_observations"
    )

# COMMAND ----------

# MAGIC %md #Procedures

# COMMAND ----------

procedures_schema = StructType([
    StructField("START", TimestampType(), True),
    StructField("STOP", TimestampType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("BASE_COST", DoubleType(), True),
    StructField("REASONCODE", StringType(), True),
    StructField("REASONDESCRIPTION", StringType(), True)
])

# COMMAND ----------

procedures_df = spark.read \
    .option("header", "true") \
    .schema(procedures_schema) \
    .csv(
        "/Volumes/healthcare_catalog/bronze/raw/procedures.csv"
    )

# COMMAND ----------

from pyspark.sql.functions import col

procedures_df = spark.read \
    .option("header", "true") \
    .schema(procedures_schema) \
    .csv("/Volumes/healthcare_catalog/source_data/raw/procedures.csv")

procedures_bronze = procedures_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *procedures_df.columns), 256))

# COMMAND ----------

procedures_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_procedures"
    )

# COMMAND ----------

# MAGIC %md #Allergies

# COMMAND ----------

allergies_schema = StructType([
    StructField("START", DateType(), True),
    StructField("STOP", DateType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True)
])

# COMMAND ----------

allergies_df = spark.read \
    .option("header", "true") \
    .schema(allergies_schema) \
    .csv(
        "/Volumes/healthcare_catalog/bronze/raw/allergies.csv"
    )

# COMMAND ----------

from pyspark.sql.functions import col

allergies_df = spark.read \
    .option("header", "true") \
    .schema(allergies_schema) \
    .csv("/Volumes/healthcare_catalog/source_data/raw/allergies.csv")

allergies_bronze = allergies_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *allergies_df.columns), 256))

# COMMAND ----------

allergies_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_allergies"
    )

# COMMAND ----------

# MAGIC %md #Immunizations

# COMMAND ----------

immunizations_schema = StructType([
    StructField("DATE", TimestampType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("BASE_COST", DoubleType(), True)
])

# COMMAND ----------

immunizations_df = spark.read \
    .option("header", "true") \
    .schema(immunizations_schema) \
    .csv(
        "/Volumes/healthcare_catalog/bronze/raw/immunizations.csv"
    )

# COMMAND ----------

from pyspark.sql.functions import col

immunizations_df = spark.read \
    .option("header", "true") \
    .schema(immunizations_schema) \
    .csv("/Volumes/healthcare_catalog/source_data/raw/immunizations.csv")

immunizations_bronze = immunizations_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *immunizations_df.columns), 256))

# COMMAND ----------

immunizations_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_immunizations"
    )

# COMMAND ----------

# MAGIC %md #Careplans

# COMMAND ----------

careplans_schema = StructType([
    StructField("Id", StringType(), True),
    StructField("START", DateType(), True),
    StructField("STOP", DateType(), True),
    StructField("PATIENT", StringType(), True),
    StructField("ENCOUNTER", StringType(), True),
    StructField("CODE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("REASONCODE", StringType(), True),
    StructField("REASONDESCRIPTION", StringType(), True)
])

# COMMAND ----------

careplans_df = spark.read \
    .option("header", "true") \
    .schema(careplans_schema) \
    .csv(
        "/Volumes/healthcare_catalog/bronze/raw/careplans.csv"
    )

# COMMAND ----------

from pyspark.sql.functions import col

careplans_df = spark.read \
    .option("header", "true") \
    .schema(careplans_schema) \
    .csv("/Volumes/healthcare_catalog/source_data/raw/careplans.csv")

careplans_bronze = careplans_df \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_ingest_date", current_date()) \
    .withColumn("_source_file", col("_metadata.file_path")) \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_record_hash", sha2(concat_ws("||", *careplans_df.columns), 256))

# COMMAND ----------

careplans_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "healthcare_catalog.bronze.brz_careplans"
    )

# COMMAND ----------

# MAGIC %md #Check all Bronze tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN healthcare_catalog.bronze

# COMMAND ----------

# MAGIC %md #Check record counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'patients' AS table_name,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM healthcare_catalog.bronze.brz_patients
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'encounters',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_encounters
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'conditions',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_conditions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'medications',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_medications
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'observations',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_observations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'procedures',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_procedures
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'allergies',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_allergies
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'immunizations',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_immunizations
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'careplans',
# MAGIC     COUNT(*)
# MAGIC FROM healthcare_catalog.bronze.brz_careplans;

# COMMAND ----------

# MAGIC %md #Check ingestion metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     _batch_id,
# MAGIC     _ingest_date,
# MAGIC     _source_file,
# MAGIC     COUNT(*) AS records
# MAGIC FROM healthcare_catalog.bronze.brz_patients
# MAGIC GROUP BY
# MAGIC     _batch_id,
# MAGIC     _ingest_date,
# MAGIC     _source_file;

# COMMAND ----------

