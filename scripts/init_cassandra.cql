-- 1. Crear el espacio de trabajo (Keyspace)
CREATE KEYSPACE IF NOT EXISTS spotify_analytics 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE spotify_analytics;

-- 2. Crear la tabla columnar mapeada con el dataset de Spotify
CREATE TABLE IF NOT EXISTS streams_by_country_genre (
    country text,
    genre text,
    stream_count bigint,
    track_id text,
    track_name text,
    artist_name text,
    album_name text,
    release_date text,
    popularity int,
    danceability double,
    energy double,
    tempo double,
    label text,
    PRIMARY KEY ((country), genre, stream_count, track_id)
) WITH CLUSTERING ORDER BY (genre ASC, stream_count DESC, track_id ASC);