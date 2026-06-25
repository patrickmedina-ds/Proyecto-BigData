// Seleccionar o crear la base de datos del proyecto
db = db.getSiblingDB('spotify_db');

// Crear la colección para almacenar los JSONs crudos del streaming
if (!db.getCollectionNames().includes('raw_events')) {
  db.createCollection('raw_events');
}

// Crear índices estratégicos para optimizar las consultas analíticas del Dashboard
db.raw_events.createIndex({ "track_id": 1 });
db.raw_events.createIndex({ "artist_name": 1 });
db.raw_events.createIndex({ "genre": 1 });
db.raw_events.createIndex({ "country": 1 });

print("Estructura e índices de MongoDB inicializados con éxito");
