import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

# Configuración de la página de Streamlit
st.set_page_config(page_title="Spotify Big Data Analytics - UTEC", layout="wide")
st.title(" Spotify Music Analytics Dashboard (2015–2025)")
st.markdown("---")

# FUNCIONES DE CONEXIÓN Y EXTRACCIÓN DE DATOS (Usando Localhost)
@st.cache_data(ttl=10)  # Sincronización y refresca la data cada 10 segundos
def fetch_mongo_metrics():
    """KPI 1: Conteo total de documentos originales almacenados en MongoDB"""
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        db = client["spotify_db"]
        total_records = db["raw_events"].count_documents({})
        client.close()
        return total_records
    except Exception:
        return 0

@st.cache_data(ttl=10)
def fetch_cassandra_data():
    """KPI 2 y 4: Extrae datos estructurados de series temporales de Cassandra"""
    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect('spotify_analytics')
        
        # Consultar todos los registros mapeados
        query = "SELECT country, genre, stream_count, popularity, track_name, artist_name, release_date FROM streams_by_country_genre;"
        rows = session.execute(query)
        df = pd.DataFrame(list(rows))
        cluster.shutdown()
        return df
    except Exception as e:
        st.sidebar.error(f"Error Cassandra: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=10)
def fetch_neo4j_metrics():
    """KPI 3 y 5: Consultas de red/grafos complejas sobre centralidad y conexiones"""
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password123"))
        
        # Query para encontrar a los artistas con mayor número de conexiones (Canciones lanzadas)
        query = """
        MATCH (a:Artist)-[:PERFORMS]->(t:Track)
        RETURN a.name AS artist, count(t) AS total_tracks
        ORDER BY total_tracks DESC LIMIT 10
        """
        with driver.session() as session:
            result = session.run(query)
            df = pd.DataFrame([dict(record) for record in result])
        driver.close()
        return df
    except Exception as e:
        st.sidebar.error(f"Error Neo4j: {e}")
        return pd.DataFrame()

# CARGA DE DATOS
total_mongo = fetch_mongo_metrics()
df_cassandra = fetch_cassandra_data()
df_neo4j = fetch_neo4j_metrics()

# DISEÑO DE LA INTERFAZ GRÁFICA (FRONTEND)

# Fila 1: Tarjetas de Métricas Principales (KPIs globales)
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(label="📁 Total Eventos en MongoDB (Crudo)", value=f"{total_mongo:,}")
with col2:
    total_streams = df_cassandra['stream_count'].sum() if not df_cassandra.empty else 0
    st.metric(label="🎧 Reproducciones Totales Indexadas (Cassandra)", value=f"{total_streams:,}")
with col3:
    total_artists = df_neo4j['artist'].nunique() if not df_neo4j.empty else 0
    st.metric(label="🎙️ Artistas Únicos en la Red (Neo4j)", value=total_artists)

st.markdown("---")

# Fila 2: Gráficos Interactivos
if not df_cassandra.empty:
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader(" Distribución de Popularidad por Género Musical")
        # Gráfico interactivo de Plotly Express
        fig_box = px.box(df_cassandra, x="genre", y="popularity", color="genre",
                         title="Análisis de dispersión de popularidad por categoría")
        st.plotly_chart(fig_box, use_container_width=True)

    with right_col:
        st.subheader(" Volumen de Streaming Global por País")
        # Agrupación dinámica con Pandas
        df_country = df_cassandra.groupby("country")["stream_count"].sum().reset_index()
        fig_pie = px.pie(df_country, values="stream_count", names="country", 
                         title="Porcentaje de participación de mercado por región")
        st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("---")

# Fila 3: Análisis de Red (Neo4j)
if not df_neo4j.empty:
    st.subheader(" Top 10 Artistas con Mayor Densidad de Conexiones en el Grafo")
    fig_bar = px.bar(df_neo4j, x="total_tracks", y="artist", orientation='h',
                     color="total_tracks", color_continuous_scale="Viridis",
                     labels={"total_tracks": "Número de Canciones Enlazadas", "artist": "Artista"},
                     title="Centralidad de Grado: Entidades con más interacciones estructurales")
    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.warning("Aún no hay suficientes relaciones cargadas en Neo4j para procesar el análisis de red.")