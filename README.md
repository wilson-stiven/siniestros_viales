# siniestros_viales
Proyecto de procesamiento de siniestros viales con Kafka y Spar
Perfecto 🔥 Aquí tienes un **README.md completo y profesional**, listo para que lo copies y pegues en tu repositorio de GitHub (`siniestros_viales`).
Solo reemplaza tu nombre en la parte final si quieres personalizarlo.

---

````markdown
# 🚦 Proyecto: Procesamiento de Siniestros Viales con Kafka y Spark

Este proyecto implementa una solución de **procesamiento de datos en tiempo real** utilizando **Apache Kafka** y **Apache Spark Streaming**, aplicándolo al análisis de siniestros viales en la ciudad de Bogotá.  

---

## 🧩 Descripción del Proyecto

El sistema permite leer un conjunto de datos con información sobre siniestros viales, enviar los registros a un **topic de Kafka**, y procesarlos en tiempo real usando **Spark Structured Streaming**.  
Con ello se pueden realizar análisis como:
- Conteo de siniestros por tipo.  
- Análisis por localidad.  
- Filtrado de siniestros con víctimas o condiciones específicas.  

---

## 🗂️ Conjunto de Datos

El dataset utilizado es **`siniestros_viales_bogota.csv`**, que contiene columnas como:
- Fecha del siniestro  
- Tipo de accidente  
- Localidad  
- Número de heridos y fallecidos  
- Condiciones climáticas  

📍 **Fuente:** Datos abiertos de la Alcaldía de Bogotá (o fuente utilizada en tu práctica).



## 🖥️ Requisitos del Entorno

* Ubuntu o Linux (VirtualBox recomendado)
* Apache Kafka instalado
* Apache Spark con PySpark
* Python 3.8 o superior

---

## 🚀 Pasos para la Ejecución

### 1️⃣ Iniciar Kafka

```bash
/opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
```

### 2️⃣ Crear el Topic

```bash
/opt/Kafka/bin/kafka-topics.sh --create --topic siniestros_viales --bootstrap-server localhost:9092
```

### 3️⃣ Enviar Datos al Topic

Puedes enviar datos manualmente:

```bash
/opt/Kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic siniestros_viales
```

O automatizarlo con un script (`kafka_setup.sh`):

```bash
bash kafka_setup.sh
```

### 4️⃣ Ejecutar Spark Streaming

```bash
python3 spark_kafka_stream.py
```

### 5️⃣ Visualizar los Resultados

En la terminal verás la salida del DataFrame en tiempo real, con los siniestros procesados y los análisis generados.

---

## 📊 Explicación del DataFrame

Un **DataFrame** en Spark es una estructura tabular distribuida (similar a una tabla SQL).
En este proyecto:

* Cada mensaje que llega desde Kafka se convierte en una **fila del DataFrame**.
* Se aplican transformaciones (`select`, `groupBy`, `agg`, etc.) para analizar los datos.
* Las operaciones se ejecutan en paralelo, aprovechando el motor distribuido de Spark.

Ejemplo:

```python
df_siniestros = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "siniestros_viales") \
    .load()

df_siniestros.selectExpr("CAST(value AS STRING)").show()
```

---

## 📷 Resultados Esperados

* Conteo de siniestros por tipo o localidad.
* Filtrado de registros con condiciones específicas.
* Visualización en consola o almacenamiento en archivos/parquet.


---

## Autor

**Wilson Stiven rojas diaz**
Proyecto académico – Procesamiento de datos en tiempo real con Apache Kafka y Spark Streaming.
Universidad / Curso: big data

