from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------
# 1️⃣ Azure Event Hub Configuration
# -----------------------------
event_hub_namespace = "<<host_name>>"
event_hub_name = "<<event_hub_name>>"

# Get connection string from Databricks Key Vault
event_hub_conn_str = dbutils.secrets.get(
    scope="hospitalanalyticsscope", key="eventhub-connection"
).strip()

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# -----------------------------
# 2️⃣ Configure Azure Data Lake Storage
# -----------------------------
storage_account_key = dbutils.secrets.get(
   <<scope=>> ,<<key=>>).strip()

spark.conf.set(
    "fs.azure.account.key.storage_account_name.dfs.core.windows.net",
    storage_account_key
)

# Optional: Test ADLS access
try:
    dbutils.fs.ls("abfss://container@storage.dfs.core.windows.net/")
    print("✅ ADLS access OK")
except Exception as e:
    print("❌ ADLS access error:", e)

# -----------------------------
# 3️⃣ Read stream from Event Hub
# -----------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Cast Kafka value from binary to string
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# -----------------------------
# 4️⃣ Write stream to Delta Bronze
# -----------------------------
bronze_path = "abfss://container_name@storage_account_)name.dfs.core.windows.net/patient-flow"
checkpoint_path = f"{bronze_path}/_checkpoint"

stream = (
    json_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)

# Optional: block the notebook until stream stops
# stream.awaitTermination()
