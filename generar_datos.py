
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

print("="*80)
print("GENERADOR DE DATASET - ACCIDENTALIDAD VIAL BOGOTÁ")
print("="*80)

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

# Número de registros a generar
NUM_REGISTROS = 10000  # Cambia a 50000 si tu VM tiene suficiente RAM

# Semilla para reproducibilidad
np.random.seed(42)

print(f"\nGenerando {NUM_REGISTROS:,} registros de accidentes...")

# =============================================================================
# DATOS DE REFERENCIA
# =============================================================================

LOCALIDADES = [
    'Suba', 'Kennedy', 'Engativá', 'Usaquén', 'Bosa', 
    'Ciudad Bolívar', 'Fontibón', 'Chapinero', 'Teusaquillo', 
    'San Cristóbal', 'Rafael Uribe', 'Santa Fe', 'Barrios Unidos',
    'Puente Aranda', 'Los Mártires', 'Antonio Nariño', 'Tunjuelito',
    'La Candelaria', 'Sumapaz'
]

TIPOS_ACCIDENTE = [
    'Choque', 'Atropello', 'Caída de Ocupante', 
    'Volcamiento', 'Incendio', 'Otro'
]

GRAVEDADES = ['Con heridos', 'Solo daños', 'Con muertos']

TIPOS_VEHICULO = [
    'Automóvil', 'Motocicleta', 'Bus', 'Camión', 
    'Bicicleta', 'Taxi', 'Buseta', 'Camioneta'
]

CLIMAS = ['Despejado', 'Lluvia', 'Nublado', 'Niebla', 'Llovizna']

ESTADOS_VIA = ['Seca', 'Húmeda', 'Con baches', 'En obras', 'Inundada']

CAUSAS = [
    'Exceso de velocidad', 'Imprudencia del conductor',
    'Embriaguez', 'Falla mecánica', 'Imprudencia del peatón',
    'Desacato a señales', 'Invasión de carril', 'Distracción',
    'Adelantamiento indebido', 'No guardar distancia'
]

GENEROS = ['Masculino', 'Femenino']

TIPOS_SERVICIO = ['Particular', 'Público', 'Oficial', 'Carga']

DIAS_SEMANA = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']

# =============================================================================
# GENERACIÓN DE DATOS
# =============================================================================

print("\n[1/4] Generando fechas y horas...")

# Generar fechas desde 2023 hasta hoy
fecha_inicio = datetime(2023, 1, 1)
fecha_fin = datetime.now()
total_dias = (fecha_fin - fecha_inicio).days

# Generar fechas aleatorias
fechas_random = [fecha_inicio + timedelta(days=np.random.randint(0, total_dias)) 
                 for _ in range(NUM_REGISTROS)]
fechas_random.sort()  # Ordenar cronológicamente

# Generar horas (más accidentes en horas pico: 7-9 AM y 5-7 PM)
def generar_hora_pico():
    if np.random.random() < 0.4:  # 40% en horas pico
        if np.random.random() < 0.5:
            hora = np.random.randint(7, 10)  # Mañana
        else:
            hora = np.random.randint(17, 20)  # Tarde
    else:
        hora = np.random.randint(0, 24)
    
    minuto = np.random.randint(0, 60)
    segundo = np.random.randint(0, 60)
    return f"{hora:02d}:{minuto:02d}:{segundo:02d}"

horas = [generar_hora_pico() for _ in range(NUM_REGISTROS)]

print("\n[2/4] Generando ubicaciones y detalles...")

# Coordenadas de Bogotá (aproximadas)
latitudes = np.random.uniform(4.5, 4.8, NUM_REGISTROS)
longitudes = np.random.uniform(-74.2, -74.0, NUM_REGISTROS)

# Generar direcciones
direcciones = [f"Calle {np.random.randint(1, 200)} con Carrera {np.random.randint(1, 150)}" 
               for _ in range(NUM_REGISTROS)]

print("\n[3/4] Generando características de accidentes...")

# Localidades (algunas más frecuentes que otras)
localidades = np.random.choice(
    LOCALIDADES, 
    NUM_REGISTROS, 
    p=[0.12, 0.11, 0.10, 0.09, 0.08, 0.07, 0.07, 0.06, 
       0.05, 0.05, 0.04, 0.04, 0.03, 0.03, 0.02, 0.02, 
       0.01, 0.005, 0.005]
)

# Tipos de accidente
tipos_accidente = np.random.choice(
    TIPOS_ACCIDENTE,
    NUM_REGISTROS,
    p=[0.45, 0.25, 0.12, 0.08, 0.05, 0.05]
)

# Gravedades (mayoría solo daños)
gravedades = np.random.choice(
    GRAVEDADES,
    NUM_REGISTROS,
    p=[0.35, 0.60, 0.05]
)

# Generar víctimas según gravedad
num_heridos = []
num_muertos = []

for gravedad in gravedades:
    if gravedad == 'Solo daños':
        num_heridos.append(0)
        num_muertos.append(0)
    elif gravedad == 'Con heridos':
        num_heridos.append(np.random.choice([1, 2, 3, 4, 5], p=[0.50, 0.25, 0.15, 0.07, 0.03]))
        num_muertos.append(0)
    else:  # Con muertos
        num_heridos.append(np.random.choice([0, 1, 2, 3], p=[0.30, 0.40, 0.20, 0.10]))
        num_muertos.append(np.random.choice([1, 2, 3], p=[0.80, 0.15, 0.05]))

# Tipos de vehículo (más motos y autos)
tipos_vehiculo = np.random.choice(
    TIPOS_VEHICULO,
    NUM_REGISTROS,
    p=[0.30, 0.35, 0.10, 0.08, 0.07, 0.07, 0.02, 0.01]
)

# Clima (mayoría despejado)
climas = np.random.choice(
    CLIMAS,
    NUM_REGISTROS,
    p=[0.50, 0.25, 0.15, 0.05, 0.05]
)

# Estado de vía
estados_via = np.random.choice(
    ESTADOS_VIA,
    NUM_REGISTROS,
    p=[0.55, 0.25, 0.12, 0.05, 0.03]
)

# Causas
causas = np.random.choice(CAUSAS, NUM_REGISTROS)

# Género (más hombres involucrados estadísticamente)
generos = np.random.choice(GENEROS, NUM_REGISTROS, p=[0.72, 0.28])

# Edades (distribución realista)
edades = np.random.choice(
    range(18, 75),
    NUM_REGISTROS,
    p=np.array([0.02] * 7 + [0.08] * 10 + [0.04] * 20 + [0.02] * 20)
)

# Tipo de servicio
tipos_servicio = np.random.choice(
    TIPOS_SERVICIO,
    NUM_REGISTROS,
    p=[0.65, 0.25, 0.05, 0.05]
)

# Días de la semana
dias_semana = [fecha.strftime('%A') for fecha in fechas_random]
# Traducir a español
traduccion_dias = {
    'Monday': 'Lunes',
    'Tuesday': 'Martes',
    'Wednesday': 'Miércoles',
    'Thursday': 'Jueves',
    'Friday': 'Viernes',
    'Saturday': 'Sábado',
    'Sunday': 'Domingo'
}
dias_semana = [traduccion_dias[dia] for dia in dias_semana]

print("\n[4/4] Creando DataFrame...")

# Crear DataFrame
df = pd.DataFrame({
    'FECHA': [fecha.strftime('%Y-%m-%d') for fecha in fechas_random],
    'HORA': horas,
    'DIA_SEMANA': dias_semana,
    'LOCALIDAD': localidades,
    'DIRECCION': direcciones,
    'LATITUD': latitudes,
    'LONGITUD': longitudes,
    'CLASE_ACCIDENTE': tipos_accidente,
    'GRAVEDAD': gravedades,
    'NUM_HERIDOS': num_heridos,
    'NUM_MUERTOS': num_muertos,
    'TIPO_VEHICULO': tipos_vehiculo,
    'CLIMA': climas,
    'ESTADO_VIA': estados_via,
    'CAUSA_PROBABLE': causas,
    'GENERO': generos,
    'EDAD': edades,
    'TIPO_SERVICIO': tipos_servicio
})

# =============================================================================
# GUARDAR ARCHIVO
# =============================================================================

print("\nGuardando archivo CSV...")

# Crear carpeta data si no existe
os.makedirs('data', exist_ok=True)

# Guardar CSV
output_file = 'data/amazon.xlsx'  # Nombre igual al de tu compañero
df.to_csv(output_file, index=False, encoding='utf-8')

# =============================================================================
# ESTADÍSTICAS DEL DATASET
# =============================================================================

print("\n" + "="*80)
print("✅ DATASET GENERADO EXITOSAMENTE")
print("="*80)

print(f"\n📁 Archivo creado: {output_file}")
print(f"📊 Total de registros: {len(df):,}")
print(f"💾 Tamaño del archivo: {os.path.getsize(output_file) / (1024*1024):.2f} MB")

print(f"\n📅 Período de datos:")
print(f"   Desde: {df['FECHA'].min()}")
print(f"   Hasta: {df['FECHA'].max()}")

print(f"\n🚑 Resumen de víctimas:")
print(f"   Total heridos: {df['NUM_HERIDOS'].sum():,}")
print(f"   Total muertos: {df['NUM_MUERTOS'].sum():,}")
print(f"   Total víctimas: {df['NUM_HERIDOS'].sum() + df['NUM_MUERTOS'].sum():,}")

print(f"\n📍 Localidades únicas: {df['LOCALIDAD'].nunique()}")
print(f"🚗 Tipos de vehículo: {df['TIPO_VEHICULO'].nunique()}")

print(f"\n⚠️  Distribución por gravedad:")
for gravedad, count in df['GRAVEDAD'].value_counts().items():
    porcentaje = (count / len(df)) * 100
    print(f"   {gravedad:15s}: {count:6,} ({porcentaje:5.2f}%)")

print(f"\n🏆 Top 5 localidades con más accidentes:")
for i, (localidad, count) in enumerate(df['LOCALIDAD'].value_counts().head().items(), 1):
    porcentaje = (count / len(df)) * 100
    print(f"   {i}. {localidad:20s}: {count:5,} ({porcentaje:5.2f}%)")

print("\n" + "="*80)
print("🚀 Dataset listo para ser procesado con Spark!")
print("="*80)
print("\nPróximos pasos:")
print("  1. Ejecutar: spark-submit batch_processing.py")
print("  2. Iniciar Kafka y ejecutar: python3 kafka_producer_reviews.py")
print("  3. Ejecutar: spark-submit streaming_processing.py")
print("="*80)
