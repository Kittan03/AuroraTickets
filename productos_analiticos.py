from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, sum as _sum, avg

spark = SparkSession.builder.appName("Productos_Finales_Master").getOrCreate()


rds_url = "jdbc:mysql://database-1.cdwxplytkn0z.us-east-1.rds.amazonaws.com:3306/practica?useSSL=false"
db_properties = {
    "user": "admin", 
    "password": "password2026", 
    "driver": "com.mysql.cj.jdbc.Driver",
    "batchsize": "500" # Lotes pequeños para no saturar el RDS
}


df = spark.read.parquet("s3a://danibucket2026/curated/").cache()

# --- 10.1 PRODUCTO A: Funnel de Conversión ---

producto_a = df.withColumn("dt", col("timestamp").cast("date")) \
    .groupBy("dt", "student_id").agg(
        count("*").alias("sessions_total"),
        count(when(col("event_type") == "page_view", 1)).alias("sessions_event_list"),
        count(when(col("event_type") == "product_detail", 1)).alias("sessions_event_detail"),
        count(when(col("event_type") == "begin_checkout", 1)).alias("sessions_begin_checkout"),
        count(when(col("event_type") == "purchase", 1)).alias("sessions_purchase")
    ).withColumn("conversion_rate", (col("sessions_purchase") / col("sessions_total")) * 100)

# --- 10.2 PRODUCTO B: Interés vs Ingresos ---

print("Calculando Producto B...")
producto_b = df.withColumn("dt", col("timestamp").cast("date")) \
    .groupBy("dt", "page").agg(
        count(when(col("event_type") == "product_detail", 1)).alias("detail_views"),
        count(when(col("event_type") == "purchase", 1)).alias("purchases")
    ).withColumn("revenue_total", col("purchases") * 50) \
     .withColumn("interest_to_purchase_ratio", col("purchases") / col("detail_views"))

# --- 10.3 PRODUCTO C: Detección de Anomalías ---

print("Calculando Producto C...")
producto_c = df.withColumn("dt", col("timestamp").cast("date")) \
    .groupBy("dt", "ip").agg(
        count("*").alias("requests"),
        count(when(col("event_type") == "purchase", 1)).alias("purchases")
    ).withColumn("is_anomaly", col("requests") > 50) \
     .withColumn("reason", when(col("is_anomaly"), "Frecuencia sospechosa (Posible Bot)").otherwise("Normal"))


print("Guardando en S3 Analytics...")
producto_a.write.mode("overwrite").parquet("s3a://danibucket2026/analytics/producto_a")
producto_b.write.mode("overwrite").parquet("s3a://danibucket2026/analytics/producto_b")
producto_c.write.mode("overwrite").parquet("s3a://danibucket2026/analytics/producto_c")


print("Cargando métricas finales en RDS...")
producto_a.coalesce(1).write.jdbc(url=rds_url, table="res_producto_a", mode="overwrite", properties=db_properties)
producto_b.coalesce(1).write.jdbc(url=rds_url, table="res_producto_b", mode="overwrite", properties=db_properties)
producto_c.coalesce(1).write.jdbc(url=rds_url, table="res_producto_c", mode="overwrite", properties=db_properties)

print("¡ÉXITO TOTAL! Pipeline completado.")
spark.stop()