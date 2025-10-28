PROCESAMIENTO BATCH - ACCIDENTALIDAD VIAL BOGOTÁ
Análisis de datos históricos con Apache Spark
Autor: UNAD - Ingeniería de Sistemas  
Wilson Stiven Rojas Diaz
Curso: Big Data 
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("="*80)
print("ANÁLISIS BATCH - ACCIDENTALIDAD VIAL BOGOTÁ")
print("="*80)

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("AccidentalidadBogota_Batch") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n[1/5] Cargando datos...")

# Definir esquema
schema = StructType([
    StructField("FECHA", StringType()),
    StructField("HORA", StringType()),
    StructField("DIA_SEMANA", StringType()),
    StructField("LOCALIDAD", StringType()),
    StructField("DIRECCION", StringType()),
    StructField("LATITUD", DoubleType()),
    StructField("LONGITUD", DoubleType()),
    StructField("CLASE_ACCIDENTE", StringType()),
    StructField("GRAVEDAD", StringType()),
    StructField("NUM_HERIDOS", IntegerType()),
    StructField("NUM_MUERTOS", IntegerType()),
    StructField("TIPO_VEHICULO", StringType()),
    StructField("CLIMA", StringType()),
    StructField("ESTADO_VIA", StringType()),
    StructField("CAUSA_PROBABLE", StringType()),
    StructField("GENERO", StringType()),
    StructField("EDAD", IntegerType()),
    StructField("TIPO_SERVICIO", StringType())
])

# Leer archivo CSV
# Asegúrate de tener el archivo en la ruta correcta
df = spark.read.csv(
    "data/amazon.xlsx",  # Cambia esto a tu archivo
    header=True,
    schema=schema
)

total_records = df.count()
print(f"✓ Datos cargados: {total_records:,} registros")

print("\n[2/5] Limpieza y transformación...")

# Eliminar duplicados
df = df.dropDuplicates()

# Manejar nulos
df = df.na.fill({
    "NUM_HERIDOS": 0,
    "NUM_MUERTOS": 0,
    "EDAD": 35,
    "CLIMA": "Desconocido"
})

# Convertir fecha
df = df.withColumn("FECHA", to_date(col("FECHA")))
df = df.withColumn("ANIO", year(col("FECHA")))
df = df.withColumn("MES", month(col("FECHA")))

# Extraer hora del día
df = df.withColumn("HORA_DIA", hour(to_timestamp(col("HORA"), "HH:mm:ss")))

# Clasificar franja horaria
df = df.withColumn("FRANJA_HORARIA",
    when((col("HORA_DIA") >= 6) & (col("HORA_DIA") < 12), "Mañana")
    .when((col("HORA_DIA") >= 12) & (col("HORA_DIA") < 18), "Tarde")
    .when((col("HORA_DIA") >= 18) & (col("HORA_DIA") < 24), "Noche")
    .otherwise("Madrugada")
)

# Total víctimas
df = df.withColumn("TOTAL_VICTIMAS", col("NUM_HERIDOS") + col("NUM_MUERTOS"))

# Grupo de edad
df = df.withColumn("GRUPO_EDAD",
    when(col("EDAD") < 25, "Joven (18-24)")
    .when((col("EDAD") >= 25) & (col("EDAD") < 40), "Adulto (25-39)")
    .when((col("EDAD") >= 40) & (col("EDAD") < 60), "Adulto Mayor (40-59)")
    .otherwise("Senior (60+)")
)

# Cache para mejor rendimiento
df.cache()
print("✓ Transformaciones completadas")
print(f"  Registros después de limpieza: {df.count():,}")

print("\n[3/5] Análisis con Spark SQL...")

# Crear vista temporal
df.createOrReplaceTempView("accidentes")

# ANÁLISIS 1: Top 10 localidades
print("\n📍 TOP 10 LOCALIDADES CON MÁS ACCIDENTES:")
print("-" * 80)
top_localidades = spark.sql("""
    SELECT 
        LOCALIDAD,
        COUNT(*) as total_accidentes,
        SUM(NUM_HERIDOS) as total_heridos,
        SUM(NUM_MUERTOS) as total_muertos,
        ROUND(AVG(TOTAL_VICTIMAS), 2) as promedio_victimas
    FROM accidentes
    GROUP BY LOCALIDAD
    ORDER BY total_accidentes DESC
    LIMIT 10
""")
top_localidades.show(truncate=False)

# ANÁLISIS 2: Distribución por franja horaria
print("\n⏰ DISTRIBUCIÓN POR FRANJA HORARIA:")
print("-" * 80)
franja = spark.sql("""
    SELECT 
        FRANJA_HORARIA,
        COUNT(*) as total_accidentes,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
    FROM accidentes
    GROUP BY FRANJA_HORARIA
    ORDER BY total_accidentes DESC
""")
franja.show()

# ANÁLISIS 3: Accidentes por día de la semana
print("\n📅 ACCIDENTES POR DÍA DE LA SEMANA:")
print("-" * 80)
dias = spark.sql("""
    SELECT 
        DIA_SEMANA,
        COUNT(*) as total_accidentes,
        SUM(NUM_MUERTOS) as total_muertos
    FROM accidentes
    GROUP BY DIA_SEMANA
    ORDER BY total_accidentes DESC
""")
dias.show()

# ANÁLISIS 4: Por tipo de vehículo
print("\n🚗 ACCIDENTES POR TIPO DE VEHÍCULO:")
print("-" * 80)
vehiculo = spark.sql("""
    SELECT 
        TIPO_VEHICULO,
        COUNT(*) as total_accidentes,
        SUM(NUM_MUERTOS) as total_muertos,
        ROUND(SUM(NUM_MUERTOS) * 100.0 / COUNT(*), 2) as tasa_mortalidad
    FROM accidentes
    GROUP BY TIPO_VEHICULO
    ORDER BY total_accidentes DESC
""")
vehiculo.show()

# ANÁLISIS 5: Correlación clima-accidentes
print("\n🌦️  ACCIDENTES POR CONDICIÓN CLIMÁTICA:")
print("-" * 80)
clima = spark.sql("""
    SELECT 
        CLIMA,
        COUNT(*) as total_accidentes,
        SUM(NUM_HERIDOS) as heridos,
        SUM(NUM_MUERTOS) as muertos
    FROM accidentes
    GROUP BY CLIMA
    ORDER BY total_accidentes DESC
""")
clima.show()

# ANÁLISIS 6: Gravedad de accidentes
print("\n⚠️  ANÁLISIS POR GRAVEDAD:")
print("-" * 80)
gravedad = spark.sql("""
    SELECT 
        GRAVEDAD,
        COUNT(*) as cantidad,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
    FROM accidentes
    GROUP BY GRAVEDAD
    ORDER BY cantidad DESC
""")
gravedad.show()

print("\n[4/5] Análisis con RDDs...")

# Convertir a RDD
rdd = df.rdd

# Análisis 1: Top 5 causas usando map/reduce
print("\n🔍 TOP 5 CAUSAS DE ACCIDENTES (usando RDD):")
print("-" * 80)
causas = rdd \
    .map(lambda r: (r.CAUSA_PROBABLE, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(5)

for i, (causa, count) in enumerate(causas, 1):
    print(f"  {i}. {causa}: {count:,} accidentes")

# Análisis 2: Accidentes fatales por localidad
print("\n💀 ACCIDENTES FATALES POR LOCALIDAD (usando RDD):")
print("-" * 80)
fatales = rdd \
    .filter(lambda r: r.NUM_MUERTOS > 0) \
    .map(lambda r: (r.LOCALIDAD, r.NUM_MUERTOS)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(5)

for i, (localidad, muertos) in enumerate(fatales, 1):
    print(f"  {i}. {localidad}: {muertos} fallecidos")

# Análisis 3: Promedio de edad por tipo de vehículo
print("\n👤 EDAD PROMEDIO POR TIPO DE VEHÍCULO (usando RDD):")
print("-" * 80)
edad_vehiculo = rdd \
    .map(lambda r: (r.TIPO_VEHICULO, (r.EDAD, 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: round(x[0] / x[1], 1)) \
    .collect()

for vehiculo, edad_prom in sorted(edad_vehiculo, key=lambda x: x[1], reverse=True):
    print(f"  • {vehiculo}: {edad_prom} años")

print("\n[5/5] Guardando resultados...")

# Crear carpeta de resultados si no existe
import os
os.makedirs("resultados", exist_ok=True)

# Guardar análisis principales
top_localidades.write.mode("overwrite").csv(
    "resultados/top_localidades", 
    header=True
)
print("✓ Top localidades guardado")

# Guardar datos procesados en Parquet
df.write.mode("overwrite").parquet("resultados/datos_procesados")
print("✓ Datos procesados guardados en Parquet")

# Resumen ejecutivo
resumen = spark.sql("""
    SELECT 
        COUNT(*) as total_accidentes,
        SUM(NUM_HERIDOS) as total_heridos,
        SUM(NUM_MUERTOS) as total_muertos,
        SUM(TOTAL_VICTIMAS) as total_victimas,
        ROUND(AVG(EDAD), 1) as edad_promedio,
        COUNT(DISTINCT LOCALIDAD) as localidades_afectadas
    FROM accidentes
""")

print("\n" + "="*80)
print("RESUMEN EJECUTIVO")
print("="*80)
resumen.show(truncate=False)

# Guardar resumen
resumen.write.mode("overwrite").csv("resultados/resumen_ejecutivo", header=True)
print("✓ Resumen ejecutivo guardado")

print("\n" + "="*80)
print("✅ PROCESAMIENTO BATCH COMPLETADO EXITOSAMENTE")
print("="*80)
print("\nArchivos generados:")
print("  1. resultados/top_localidades/ - Top localidades CSV")
print("  2. resultados/datos_procesados/ - Dataset en Parquet")
print("  3. resultados/resumen_ejecutivo/ - Resumen CSV")
print("\n🚀 Listo para procesamiento streaming!")
print("="*80)

# Detener sesión Spark
spark.stop()
