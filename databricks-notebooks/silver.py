from pyspark.sql.types import *
from pyspark.sql.functions import *
storage_account_key = dbutils.secrets.get(<<scope=>>,<<key=>>).strip()

spark.conf.set(
    "fs.azure.account.key.<<storage_name>>.dfs.core.windows.net",
    storage_account_key
)
#bronze layer path
bronze_path = "abfss://comtainer_name@storage_account_name.dfs.core.windows.net/patient-flow"
silver_path = "abfss://container_name@storage_account_name.dfs.core.windows.net/patient-flow"
dbutils.fs.rm(silver_path + "/_checkpoint", recurse=True)


#read data from the bronze 
bronze_df =  spark.readStream.format("delta").load(bronze_path)

#define the schema for the incoming data
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", TimestampType()),
    StructField("discharge_time", TimestampType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())

])


#parse it to the dataframe 
parsed_df = bronze_df.withColumn("data",from_json(col("raw_json"),schema)).select("data.*")

#convert type the timestamp
clean_df = parsed_df.withColumn("admission_time",to_timestamp(col("admission_time")))\
                    .withColumn("discharge_time",to_timestamp(col("discharge_time")))

# invalid admision time
clean_df = clean_df.withColumn("admission_time",
                                when(
                                   col("admission_time").isNull() | (col("admission_time")>current_timestamp()),
                                   current_timestamp()
                                    )
                                .otherwise(col("admission_time"))
                               )

#handle ivalid age 
"""clean_df = clean_df.withColumn("age",
                               when(
                                   col("age")>100,floor(random()*90+1).cast("int")
                                .otherwise(col("age"))
                               
                               ))
"""
clean_df = clean_df.withColumn(
    "age",
    when(col("age").isNull(), lit(-1))                # if null → set NULL
    .when((col("age") < 0) | (col("age") > 120), lit(-1))  # invalid age → NULL
    .otherwise(col("age"))                              # else keep original
)

#schema evolution 
expected_columns = ["patient_id","gender","age","department","admission_time","discharge_time","bed_id","hospital_id"]

for col_name in expected_columns:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name,lit(None))
    
clean_df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", silver_path + "/_checkpoint") \
    .option("skipChangeCommits", "true") \
    .start(silver_path)

