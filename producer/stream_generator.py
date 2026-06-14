import os
import time
import pandas as pd
from datetime import datetime

# Configuración de rutas
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "spotify_dataset.csv")
QUEUE_DIR = os.path.join(PROJECT_ROOT, "data", "queue")
BATCH_SIZE = 100  # Cuántos registros enviar por "oleada"
INTERVAL_SECONDS = 5  # Cada cuánto tiempo enviar una oleada

def simulate_streaming():
    # 1. Verificar que el archivo existe
    if not os.path.exists(CSV_PATH):
        print(f"Error: No se encontró el archivo en {CSV_PATH}")
        return

    # 2. Crear la carpeta queue si no existe
    os.makedirs(QUEUE_DIR, exist_ok=True)

    print("Iniciando simulación de streaming de Spotify...")
    
    # 3. Leer el CSV por trozos (chunks) para no saturar la memoria
    chunk_iterator = pd.read_csv(CSV_PATH, chunksize=BATCH_SIZE)

    for i, chunk in enumerate(chunk_iterator):
        # Convertir el lote de filas del CSV a formato JSON (lista de diccionarios)
        records = chunk.to_dict(orient="records")
        
        # Crear un nombre de archivo único basado en el timestamp actual
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file_name = f"spotify_events_{timestamp}_{i}.json"
        json_file_path = os.path.join(QUEUE_DIR, json_file_name)
        
        # Guardar el JSON en la carpeta de intercambio (queue)
        # Usamos un dataframe intermedio para escribirlo limpio como JSON estructurado
        chunk.to_json(json_file_path, orient="records", indent=4)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Lote {i+1} enviado: {json_file_name} ({len(records)} registros).")
        
        # Esperar antes de enviar el siguiente lote
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    simulate_streaming()
