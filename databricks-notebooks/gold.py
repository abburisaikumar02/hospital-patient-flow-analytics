from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ------------------------------
# ADLS Storage Setup
# ------------------------------
storage_account_key = dbutils.secrets.get(<<scope=>>,<<key=>>).strip()

spark.conf.set(
    "fs.azure.account.key.storage_account_name.dfs.core.windows.net",
    storage_account_key
)

silver_path = "abfss://container_name@storage_name.dfs.core.windows.net/patient-flow"
gold_dim_patient = "abfss://container_name@storage_name.dfs.core.windows.net/dim_patient"
gold_dim_department = "abfss://container_name@storage_name.dfs.core.windows.net/dim_departments"
gold_fact = "abfss://container_name@storage_name.core.windows.net/fact_patient_flow"

# Ensure directories exist (avoids PATH_NOT_FOUND)
dbutils.fs.mkdirs("abfss://container_name@storage_name.dfs.core.windows.net/")
dbutils.fs.mkdirs(gold_dim_patient)
dbutils.fs.mkdirs(gold_dim_department)
dbutils.fs.mkdirs(gold_fact)

# ------------------------------
# Load Silver Table
# ------------------------------
silver_df = spark.read.format("delta").load(silver_path)

# Keep latest admission per patient
w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())
silver_df = silver_df.withColumn("rank", F.row_number().over(w)) \
                     .filter(F.col("rank") == 1) \
                     .drop("rank")

# ------------------------------
# Patient Dimension (SCD2)
# ------------------------------
incoming_patient = silver_df.select("patient_id", "gender", "age") \
    .withColumn("effective_from", current_timestamp()) \
    .withColumn("_hash", F.sha2(F.concat_ws("||",
                                            F.coalesce(col("gender"), lit("NA")),
                                            F.coalesce(col("age").cast("string"), lit("NA"))
                                           ), 256))

# Initialize patient dim if not exists
if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
    incoming_patient.withColumn("surrogate_key", F.monotonically_increasing_id()) \
                    .withColumn("effective_to", lit(None).cast("timestamp")) \
                    .withColumn("is_current", lit(True)) \
                    .write.format("delta").mode("overwrite").save(gold_dim_patient)

# Load target DeltaTable
target_patient = DeltaTable.forPath(spark, gold_dim_patient)

# SCD2 Merge (update old + insert new)
target_patient.alias("t").merge(
    incoming_patient.alias("i"),
    "t.patient_id = i.patient_id"
).whenMatchedUpdate(
    condition="t.is_current = True AND t._hash <> t._hash",
    set={
        "is_current": lit(False),
        "effective_to": current_timestamp()
    }
).whenNotMatchedInsert(
    values={
        "surrogate_key": F.monotonically_increasing_id(),
        "patient_id": col("i.patient_id"),
        "gender": col("i.gender"),
        "age": col("i.age"),
        "is_current": lit(True),
        "effective_from": col("i.effective_from"),
        "effective_to": lit(None),
        "_hash": col("_hash")
    }
).execute()

# ------------------------------
# Department Dimension
# ------------------------------
incoming_dept = silver_df.select("department", "hospital_id") \
                         .dropDuplicates(["department", "hospital_id"]) \
                         .withColumn("surrogate_key", F.monotonically_increasing_id())

# Create department table if not exists
if not DeltaTable.isDeltaTable(spark, gold_dim_department):
    incoming_dept.write.format("delta").mode("overwrite").save(gold_dim_department)

# ------------------------------
# Fact Table
# ------------------------------
dim_patient_df = spark.read.format("delta").load(gold_dim_patient) \
    .filter(col("is_current") == True) \
    .select(col("surrogate_key").alias("surrogate_key_patient"), "patient_id")

dim_dept_df = spark.read.format("delta").load(gold_dim_department) \
    .select(col("surrogate_key").alias("surrogate_key_dept"), "department", "hospital_id")

fact_base = silver_df.select("patient_id", "department", "hospital_id", "admission_time", "discharge_time", "bed_id") \
                     .withColumn("admission_date", F.to_date("admission_time"))

fact_enriched = fact_base.join(dim_patient_df, on="patient_id", how="left") \
                         .join(dim_dept_df, on=["department", "hospital_id"], how="left") \
                         .withColumn("length_of_stay_hours", 
                                     (F.unix_timestamp("discharge_time") - F.unix_timestamp("admission_time")) / 3600.0) \
                         .withColumn("is_currently_admitted", F.when(col("discharge_time") > current_timestamp(), True).otherwise(False)) \
                         .withColumn("event_ingestion_time", current_timestamp())

fact_final = fact_enriched.select(
    F.monotonically_increasing_id().alias("fact_id"),
    col("surrogate_key_patient").alias("patient_sk"),
    col("surrogate_key_dept").alias("department_sk"),
    "admission_time", "discharge_time", "admission_date",
    "length_of_stay_hours", "is_currently_admitted", "bed_id", "event_ingestion_time"
)

fact_final.write.format("delta").mode("overwrite").save(gold_fact)

# ------------------------------
# Quick Sanity Checks
# ------------------------------
print("Patient dim count:", spark.read.format("delta").load(gold_dim_patient).count())
print("Department dim count:", spark.read.format("delta").load(gold_dim_department).count())
print("Fact rows:", spark.read.format("delta").load(gold_fact).count())
