from pyspark.sql import SparkSession

# 1. Inicialización de la sesión de Spark
spark = SparkSession.builder \
    .appName("Job2_S3_to_RDS_Final") \
    .getOrCreate()

# 2. Lectura de los datos Parquet (Desde tu carpeta local en la EC2-5)
# Estos son los datos que el Job 1 procesó con éxito
path_local = "file:///home/ubuntu/curated_clickstream/"
print(f"Leyendo datos curados desde: {path_local}")
df = spark.read.parquet(path_local)



rds_host = "database-1.cdwxplytkn0z.us-east-1.rds.amazonaws.com"
db_name = "practica"  # Si tu BD tiene otro nombre, cámbialo aquí
db_user = "admin"
db_pass = "password2026"


rds_url = f"jdbc:mysql://{rds_host}:3306/{db_name}?createDatabaseIfNotExist=True&useSSL=false"

db_properties = {
    "user": db_user,
    "password": db_pass,
    "driver": "com.mysql.cj.jdbc.Driver"
}


try:
    print(f"Intentando conectar a RDS: {rds_host}...")
  
    df.write.jdbc(url=rds_url, table="clickstream_final", mode="overwrite", properties=db_properties)
    print("--------------------------------------------------")
    print("¡ÉXITO TOTAL! Los datos se han cargado en RDS.")
    print("--------------------------------------------------")
except Exception as e:
    print("--------------------------------------------------")
    print(f"ERROR CRÍTICO AL CARGAR EN RDS: {e}")
    print("Revisa si el Security Group permite el puerto 3306.")
    print("--------------------------------------------------")

spark.stop()