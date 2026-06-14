from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Drivers de conexión
from pymongo import MongoClient, ReplaceOne
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from neo4j import GraphDatabase

default_args = {
    'owner': 'utec_bigdata',
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'pipeline_spotify_multimodelo',
    default_args=default_args,
    description='Pipeline ETL incremental para analítica de Spotify',
    schedule_interval=timedelta(minutes=2), # Revisa la carpeta cada 2 minutos
    catchup=False,
) as dag:

    # RUTAS DE INTERCAMBIO DE ARCHIVOS
    QUEUE_DIR = "/opt/airflow/data/queue"
    PROCESSED_DIR = "/opt/airflow/data/processed"
    

    def extract_and_process_batch(**kwargs):
        """Busca TODOS los archivos en la cola, los unifica, los limpia y los pasa a las BDs"""
        os.makedirs(QUEUE_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        # 1. Buscar TODOS los archivos JSON en la cola
        files = sorted(f for f in os.listdir(QUEUE_DIR) if f.endswith('.json'))
        if not files:
            print(f"No se encontraron nuevos archivos en la cola: {QUEUE_DIR}")
            return None

        print(f"¡Atención! Se encontraron {len(files)} archivos acumulados. Procesando todos juntos...")

        all_records = []
        
        # 2. Leer, limpiar y acumular el contenido de cada archivo
        for target_file in files:
            file_path = os.path.join(QUEUE_DIR, target_file)
            try:
                df = pd.read_json(file_path)
                
                # Limpieza de datos
                df['track_name'] = df['track_name'].fillna("Unknown Track")
                df['artist_name'] = df['artist_name'].fillna("Unknown Artist")
                df['genre'] = df['genre'].fillna("Unknown")
                df['country'] = df['country'].fillna("Global")
                df['stream_count'] = df['stream_count'].fillna(0).astype(int)
                df['popularity'] = df['popularity'].fillna(0).astype(int)
                df = df.dropna(subset=['track_id'])
                df = df.where(pd.notnull(df), None)
                
                # Unir a la lista maestra de registros
                all_records.extend(df.to_dict(orient="records"))
            except Exception as e:
                print(f"Error leyendo el archivo {target_file}: {e}")

        # Guardar la megamuestra de datos y la lista de archivos procesados en XCom
        kwargs['ti'].xcom_push(key='spotify_data', value=all_records)
        kwargs['ti'].xcom_push(key='processed_files', value=files)
        print(f"Total de registros listos para insertar en las 3 BDs: {len(all_records)}")

    def clean_up_file(**kwargs):
        """Mueve TODOS los archivos procesados a la carpeta 'processed'"""
        ti = kwargs['ti']
        files = ti.xcom_pull(key='processed_files', task_ids='extract_and_validate')
        if not files: return

        # Mover cada uno de los archivos que leímos
        for target_file in files:
            src = os.path.join(QUEUE_DIR, target_file)
            dst = os.path.join(PROCESSED_DIR, target_file)
            if os.path.exists(src):
                os.rename(src, dst)
        
        print(f"Éxito: Se limpió la cola moviendo {len(files)} archivos a 'processed'.")
    
    
    def load_to_mongo(**kwargs):
        """Guarda los JSONs crudos tal cual en MongoDB"""
        ti = kwargs['ti']
        data = ti.xcom_pull(key='spotify_data', task_ids='extract_and_validate')
        if not data: return

        # Conectar a MongoDB usando el nombre del contenedor como Host
        client = MongoClient("mongodb://mongodb:27017/")
        db = client["spotify_db"]
        collection = db["raw_events"]

        # Carga idempotente para que reintentos del DAG no dupliquen documentos.
        operations = []
        for row in data:
            track_id = str(row.get('track_id') or '').strip()
            if not track_id:
                continue
            document = dict(row)
            document['_id'] = track_id
            operations.append(ReplaceOne({'_id': track_id}, document, upsert=True))

        if operations:
            collection.bulk_write(operations, ordered=False)
        client.close()
        print(f"Éxito: {len(operations)} documentos guardados/actualizados en MongoDB.")

    def load_to_cassandra(**kwargs):
        """Transforma e inserta los datos en la tabla columnar de Cassandra"""
        ti = kwargs['ti']
        data = ti.xcom_pull(key='spotify_data', task_ids='extract_and_validate')
        if not data: return

        # Conectar a Cassandra usando el nombre del contenedor como Host
        cluster = Cluster(
            ['cassandra'],
            port=9042,
            protocol_version=5,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
        )
        session = cluster.connect()

        # Preparar la consulta de inserción (Usa las columnas exactas de tu CSV)
        query = """
            INSERT INTO spotify_analytics.streams_by_country_genre
            (country, genre, stream_count, popularity, track_id, track_name, artist_name, release_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared = session.prepare(query)

        # Insertar registro por registro de forma eficiente
        for row in data:
            session.execute(prepared, (
                str(row.get('country', 'Global')),
                str(row.get('genre', 'Unknown')),
                int(row.get('stream_count', 0)),
                int(row.get('popularity', 0)),
                str(row.get('track_id', '')),
                str(row.get('track_name', '')),
                str(row.get('artist_name', '')),
                str(row.get('release_date', ''))
            ))
        
        cluster.shutdown()
        print(f"Éxito: {len(data)} registros indexados en Cassandra.")

    def load_to_neo4j(**kwargs):
        """Construye el grafo de relaciones en Neo4j (Artista -> Cancion -> Genero)"""
        ti = kwargs['ti']
        data = ti.xcom_pull(key='spotify_data', task_ids='extract_and_validate')
        if not data: return

        # Conexión al contenedor de Neo4j
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password123"))

        # Consulta Cypher por lote para fusionar nodos y relaciones sin duplicados
        cypher_query = """
        UNWIND $rows AS row
        MERGE (a:Artist {name: row.artist_name})
        MERGE (g:Genre {name: row.genre})
        MERGE (t:Track {id: row.track_id})
        SET t.name = row.track_name,
            t.popularity = row.popularity,
            t.stream_count = row.stream_count,
            t.release_date = row.release_date,
            t.duration_ms = row.duration_ms
        MERGE (a)-[:PERFORMS]->(t)
        MERGE (t)-[:BELONGS_TO_GENRE]->(g)
        """

        rows = []
        for row in data:
            track_id = str(row.get('track_id') or '').strip()
            if not track_id:
                continue

            rows.append({
                'artist_name': str(row.get('artist_name') or 'Unknown Artist'),
                'genre': str(row.get('genre') or 'Unknown'),
                'track_id': track_id,
                'track_name': str(row.get('track_name') or 'Unknown Track'),
                'release_date': str(row.get('release_date') or ''),
                'duration_ms': int(row.get('duration_ms') or 0),
                'popularity': int(row.get('popularity') or 0),
                'stream_count': int(row.get('stream_count') or 0),
            })

        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(cypher_query, rows=rows).consume())

        driver.close()
        print(f"Éxito: {len(rows)} registros vinculados en el grafo de Neo4j.")

    # TAREAS DE AIRFLOW
    task_extract = PythonOperator(
        task_id='extract_and_validate',
        python_callable=extract_and_process_batch,
        provide_context=True
    )

    task_mongo = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongo,
        provide_context=True
    )

    task_cassandra = PythonOperator(
        task_id='load_to_cassandra',
        python_callable=load_to_cassandra,
        provide_context=True
    )

    task_cleanup = PythonOperator(
        task_id='clean_up_file',
        python_callable=clean_up_file,
        provide_context=True
    )

    task_neo4j = PythonOperator(
        task_id='load_to_neo4j',
        python_callable=load_to_neo4j,
        provide_context=True
    )

    # FLUJO DEL PIPELINE (Dejamos Neo4j para el siguiente paso)
    task_extract >> [task_mongo, task_cassandra, task_neo4j] >> task_cleanup
