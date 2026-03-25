from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Job1_Curation_Fixed").getOrCreate()


raw_df = spark.read.json("file:///home/ubuntu/clickstream.jsonl")


curated_df = raw_df.select(
    col("student_id"),
    col("event_type"),
    col("timestamp"),
    col("ip"),
    col("page")
)


print(f"DEBUG: Procesando {curated_df.count()} filas...")
curated_df.write.mode("overwrite").parquet("file:///home/ubuntu/curated_clickstream/")

print("¡Curación finalizada con éxito!")
spark.stop()