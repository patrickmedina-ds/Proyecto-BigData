// 1. Crear restricciones de unicidad para proteger la ingesta incremental del DAG
CREATE CONSTRAINT unique_track_id IF NOT EXISTS FOR (t:Track) REQUIRE t.id IS UNIQUE;
CREATE CONSTRAINT unique_artist_name IF NOT EXISTS FOR (a:Artist) REQUIRE a.name IS UNIQUE;
CREATE CONSTRAINT unique_genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE;
CREATE CONSTRAINT unique_label_name IF NOT EXISTS FOR (l:Label) REQUIRE l.name IS UNIQUE;

// 2. Crear índices tradicionales para búsquedas rápidas de texto en el Dashboard
CREATE INDEX track_name_idx IF NOT EXISTS FOR (t:Track) ON (t.name);
CREATE INDEX artist_name_idx IF NOT EXISTS FOR (a:Artist) ON (a.name);