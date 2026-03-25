from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, when, lit, sum as _sum

spark = SparkSession.builder.appName("Productos_Analiticos").getOrCreate()

rds_host = "database-1.cdwxplytkn0z.us-east-1.rds.amazonaws.com"
db_name = "practica"
db_user = "admin"
db_pass = "password2026"
rds_url = f"jdbc:mysql://{rds_host}:3306/{db_name}?useSSL=false&allowPublicKeyRetrieval=true"
db_properties = {"user": db_user, "password": db_pass, "driver": "com.mysql.cj.jdbc.Driver"}


df = spark.read.jdbc(url=rds_url, table="clickstream_final", properties=db_properties)

# --- PRODUCTO A: Funnel de Conversión ---
print("Generando Producto A...")
producto_a = df.groupBy("student_id").agg(
    count("*").alias("sessions_total"), # Simplificado a nivel estudiante
    count(when(col("event_type") == "page_view", 1)).alias("sessions_event_list"),
    count(when(col("event_type") == "product_detail", 1)).alias("sessions_event_detail"),
    count(when(col("event_type") == "begin_checkout", 1)).alias("sessions_begin_checkout"),
    count(when(col("event_type") == "purchase", 1)).alias("sessions_purchase")
).withColumn("conversion_rate", (col("sessions_purchase") / col("sessions_total")) * 100)

# --- PRODUCTO B: Interés vs Ingresos ---
print("Generando Producto B...")
producto_b = df.groupBy("student_id").agg(
    count(when(col("event_type") == "product_detail", 1)).alias("detail_views"),
    count(when(col("event_type") == "purchase", 1)).alias("purchases")
).withColumn("revenue_total", col("purchases") * 50) \
 .withColumn("interest_to_purchase_ratio", col("purchases") / col("detail_views"))

# --- PRODUCTO C: Anomalías (IPs sospechosas) ---
print("Generando Producto C...")
producto_c = df.groupBy("ip").agg(
    count("*").alias("requests"),
    count(when(col("event_type") == "purchase", 1)).alias("purchases")
).withColumn("is_anomaly", col("requests") > 100) \
 .withColumn("reason", when(col("is_anomaly"), "Exceso de peticiones (Posible Bot)").otherwise("Normal"))


print("Guardando resultados en RDS...")
producto_a.write.jdbc(url=rds_url, table="res_producto_a", mode="overwrite", properties=db_properties)
producto_b.write.jdbc(url=rds_url, table="res_producto_b", mode="overwrite", properties=db_properties)
producto_c.write.jdbc(url=rds_url, table="res_producto_c", mode="overwrite", properties=db_properties)

print("¡PROCESO FINALIZADO! Tablas res_producto_a, b y c creadas.")
spark.stop()