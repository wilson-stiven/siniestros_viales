
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# =============================================================================
# CONFIGURACIÓN DEL SIMULADOR
# =============================================================================

LOCALIDADES = [
    'Suba', 'Kennedy', 'Engativá', 'Usaquén', 'Bosa', 
    'Ciudad Bolívar', 'Fontibón', 'Chapinero', 'Teusaquillo', 
    'San Cristóbal'
]

TIPOS_ACCIDENTE = [
    'Choque', 
    'Atropello', 
    'Caída de Ocupante', 
    'Volcamiento', 
    'Incendio',
    'Otro'
]

TIPOS_VEHICULO = [
    'Automóvil', 
    'Motocicleta', 
    'Bus', 
    'Camión', 
    'Bicicleta',
    'Taxi',
    'Buseta'
]

CLIMAS = [
    'Despejado', 
    'Lluvia', 
    'Nublado', 
    'Niebla',
    'Llovizna'
]

GRAVEDADES = [
    'Solo daños', 
    'Con heridos', 
    'Con muertos'
]

# =============================================================================
# FUNCIÓN GENERADORA DE ACCIDENTES
# =============================================================================

def generar_accidente():
    """
    Genera un accidente simulado con datos realistas
    
    Returns:
        dict: Diccionario con datos del accidente
    """
    
    # Seleccionar gravedad con probabilidades realistas
    # 60% solo daños, 35% con heridos, 5% con muertos
    gravedad = random.choices(
        GRAVEDADES, 
        weights=[0.60, 0.35, 0.05]
    )[0]
    
    # Determinar número de víctimas según gravedad
    heridos = 0
    muertos = 0
    
    if gravedad == 'Con heridos':
        heridos = random.randint(1, 4)
    elif gravedad == 'Con muertos':
        muertos = random.randint(1, 2)
        heridos = random.randint(0, 3)  # Puede haber heridos también
    
    # Generar coordenadas dentro de Bogotá
    latitud = round(random.uniform(4.5, 4.8), 4)
    longitud = round(random.uniform(-74.2, -74.0), 4)
    
    # Hora actual
    ahora = datetime.now()
    hora_dia = ahora.hour
    
    # Crear evento de accidente
    accidente = {
        "timestamp": ahora.isoformat(),
        "localidad": random.choice(LOCALIDADES),
        "clase_accidente": random.choice(TIPOS_ACCIDENTE),
        "tipo_vehiculo": random.choice(TIPOS_VEHICULO),
        "clima": random.choice(CLIMAS),
        "gravedad": gravedad,
        "num_heridos": heridos,
        "num_muertos": muertos,
        "latitud": latitud,
        "longitud": longitud,
        "hora_dia": hora_dia
    }
    
    return accidente

# =============================================================================
# CONFIGURACIÓN DE KAFKA PRODUCER
# =============================================================================

print("="*80)
print("KAFKA PRODUCER - SIMULADOR DE ACCIDENTES EN TIEMPO REAL")
print("="*80)
print("\nConectando a Kafka...")

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        acks='all',  # Esperar confirmación de todos los brokers
        retries=3,   # Reintentar 3 veces si falla
        max_in_flight_requests_per_connection=1  # Garantizar orden
    )
    print("✓ Conectado a Kafka exitosamente")
    print(f"✓ Bootstrap servers: localhost:9092")
    print(f"✓ Topic: accidentes_tiempo_real")
    
except Exception as e:
    print(f"❌ Error al conectar con Kafka: {e}")
    print("\nVerifica que Kafka esté corriendo:")
    print("  sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &")
    exit(1)

# =============================================================================
# SIMULACIÓN DE EVENTOS
# =============================================================================

print("\n" + "="*80)
print("INICIANDO SIMULACIÓN DE ACCIDENTES")
print("="*80)
print("\nFormato: [#] Timestamp | Localidad | Tipo | Heridos | Muertos")
print("Presiona Ctrl+C para detener\n")

contador = 0
total_heridos = 0
total_muertos = 0

try:
    while True:
        # Generar accidente
        accidente = generar_accidente()
        
        # Enviar a Kafka
        try:
            future = producer.send('accidentes_tiempo_real', value=accidente)
            # Esperar confirmación
            record_metadata = future.get(timeout=10)
            
            # Actualizar contadores
            contador += 1
            total_heridos += accidente['num_heridos']
            total_muertos += accidente['num_muertos']
            
            # Mostrar evento enviado
            emoji_gravedad = ""
            if accidente['gravedad'] == 'Solo daños':
                emoji_gravedad = "🟢"
            elif accidente['gravedad'] == 'Con heridos':
                emoji_gravedad = "🟡"
            else:
                emoji_gravedad = "🔴"
            
            print(f"[{contador:4d}] {emoji_gravedad} "
                  f"{accidente['timestamp'][:19]} | "
                  f"{accidente['localidad']:15s} | "
                  f"{accidente['clase_accidente']:18s} | "
                  f"Heridos: {accidente['num_heridos']} | "
                  f"Muertos: {accidente['num_muertos']}")
            
            # Mostrar estadísticas cada 50 eventos
            if contador % 50 == 0:
                print("\n" + "-"*80)
                print(f"📊 ESTADÍSTICAS: {contador} eventos enviados | "
                      f"Total heridos: {total_heridos} | "
                      f"Total muertos: {total_muertos}")
                print("-"*80 + "\n")
            
        except Exception as e:
            print(f"❌ Error al enviar mensaje: {e}")
            continue
        
        # Simular diferentes velocidades según hora del día
        # Más accidentes en horas pico (7-9 AM y 5-7 PM)
        hora_actual = datetime.now().hour
        
        if (7 <= hora_actual <= 9) or (17 <= hora_actual <= 19):
            # Horas pico: más frecuente
            tiempo_espera = random.uniform(0.5, 2.0)
        else:
            # Horas normales: menos frecuente
            tiempo_espera = random.uniform(2.0, 5.0)
        
        time.sleep(tiempo_espera)

except KeyboardInterrupt:
    print("\n\n" + "="*80)
    print("✓ PRODUCER DETENIDO POR EL USUARIO")
    print("="*80)
    print(f"\n📊 RESUMEN FINAL:")
    print(f"  • Total eventos enviados: {contador}")
    print(f"  • Total heridos simulados: {total_heridos}")
    print(f"  • Total muertos simulados: {total_muertos}")
    print(f"  • Promedio heridos/evento: {total_heridos/contador if contador > 0 else 0:.2f}")
    print(f"  • Promedio muertos/evento: {total_muertos/contador if contador > 0 else 0:.2f}")
    print("\n✓ Conexión cerrada correctamente")
    print("="*80)
    producer.close()

except Exception as e:
    print(f"\n❌ Error inesperado: {e}")
    producer.close()
    exit(1)
