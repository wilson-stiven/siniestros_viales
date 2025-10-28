"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("="*80)
print("SPARK STREAMING - ANÁLISIS EN TIEMPO REAL DE ACCIDENTES")
print("="*80)

# Crear Spark Session
spark = SparkSession.builder \
    .appName("AccidentesStreamingBogota") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos JSON que llegan por Kafka
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("localidad", StringType()),
    StructField("clase_accidente", StringType()),
    StructField("tipo_vehiculo", StringType()),
    StructField("clima", StringType()),
    StructField("gravedad", StringType()),
    StructField("num_heridos", IntegerType()),
    StructField("num_muertos", IntegerType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType()),
    StructField("hora_dia", IntegerType())
])

print("\n[1/4] Conectando a Kafka...")

# Leer stream desde Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "accidentes_tiempo_real") \
    .option("startingOffsets", "latest") \
    .load()

print("✓ Conectado a Kafka topic: accidentes_tiempo_real")

print("\n[2/4] Procesando datos en tiempo real...")

# Parsear JSON del campo value de Kafka
df_parsed = df_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Convertir timestamp a formato timestamp
df_parsed = df_parsed.withColumn(
    "timestamp", 
    to_timestamp(col("timestamp"))
)

# Calcular total de víctimas
df_parsed = df_parsed.withColumn(
    "total_victimas",
    col("num_heridos") + col("num_muertos")
)

print("✓ Datos parseados correctamente")

print("\n[3/4] Calculando estadísticas en tiempo real...")

# ============================================================================
# ANÁLISIS 1: Conteo de accidentes por localidad (ventana de 1 minuto)
# ============================================================================
accidentes_por_localidad = df_parsed \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("localidad")
    ) \
    .agg(
        count("*").alias("total_accidentes"),
        sum("num_heridos").alias("total_heridos"),
        sum("num_muertos").alias("total_muertos"),
        sum("total_victimas").alias("total_victimas")
    ) \
    .orderBy(col("window").desc(), col("total_accidentes").desc())

# ============================================================================
# ANÁLISIS 2: Estadísticas por tipo de vehículo
# ============================================================================
por_vehiculo = df_parsed \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("tipo_vehiculo")
    ) \
    .agg(
        count("*").alias("accidentes"),
        sum("total_victimas").alias("victimas")
    ) \
    .orderBy(col("window").desc(), col("accidentes").desc())

# ============================================================================
# ANÁLISIS 3: Sistema de alertas para accidentes graves
# ============================================================================
accidentes_graves = df_parsed \
    .filter(col("gravedad") == "Con muertos") \
    .select(
        "timestamp",
        "localidad",
        "clase_accidente",
        "num_muertos",
        "latitud",
        "longitud"
    )

# ============================================================================
# ANÁLISIS 4: Resumen general cada 30 segundos
# ============================================================================
resumen_general = df_parsed \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "30 seconds")) \
    .agg(
        count("*").alias("total_accidentes"),
        sum("num_heridos").alias("heridos"),
        sum("num_muertos").alias("muertos"),
        sum("total_victimas").alias("victimas"),
        countDistinct("localidad").alias("localidades_afectadas")
    ) \
    .orderBy(col("window").desc())

# ============================================================================
# ANÁLISIS 5: Accidentes por clima en tiempo real
# ============================================================================
por_clima = df_parsed \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("clima")
    ) \
    .agg(
        count("*").alias("accidentes")
    ) \
    .orderBy(col("window").desc(), col("accidentes").desc())

print("\n[4/4] Iniciando streams de procesamiento...")
print("\n" + "="*80)
print("MONITOREO EN TIEMPO REAL ACTIVO")
print("="*80)
print("\nAnálisis en ejecución:")
print("  1. Accidentes por localidad (ventana 1 min)")
print("  2. Por tipo de vehículo (ventana 1 min)")
print("  3. ALERTAS de accidentes graves")
print("  4. Resumen general (ventana 30 seg)")
print("  5. Por condición climática (ventana 1 min)")
print("\nPresiona Ctrl+C para detener")
print("="*80 + "\n")

# ============================================================================
# INICIAR QUERIES DE STREAMING
# ============================================================================

# Query 1: Accidentes por localidad
query1 = accidentes_por_localidad \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Accidentes_por_Localidad") \
    .start()

# Query 2: Por tipo de vehículo
query2 = por_vehiculo \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Por_Tipo_Vehiculo") \
    .start()

# Query 3: Alertas de accidentes graves (append mode)
query3 = accidentes_graves \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .queryName("ALERTAS_GRAVES") \
    .start()

# Query 4: Resumen general
query4 = resumen_general \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Resumen_General") \
    .start()

# Query 5: Por clima
query5 = por_clima \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Por_Clima") \
    .start()

# ============================================================================
# ESPERAR TERMINACIÓN
# ============================================================================

try:
    # Esperar a que termine cualquiera de las queries
    spark.streams.awaitAnyTermination()
    
except KeyboardInterrupt:
    print("\n\n" + "="*80)
    print("✓ Streaming detenido por el usuario")
    print("="*80)
    
    # Detener todas las queries
    for query in spark.streams.active:
        print(f"Deteniendo query: {query.name}")
        query.stop()
    
    print("\n✓ Todas las queries detenidas")
    print("✓ Sesión Spark finalizada")
    
    # Detener Spark
    spark.stop()
    
except Exception as e:
    print(f"\n❌ Error en el procesamiento: {e}")
    for query in spark.streams.active:
        query.stop()
    spark.stop()
