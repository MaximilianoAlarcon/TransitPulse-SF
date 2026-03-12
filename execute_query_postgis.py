import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os

API_KEY = os.environ.get("API_511_KEY")

OPERATORS_URL = "http://api.511.org/transit/gtfsoperators"
DATAFEED_URL = "http://api.511.org/transit/datafeeds"

DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}

def select(cur,query):
    print("Ejecutando consulta")
    cur.execute(query)
    rows = cur.fetchall()  # trae todos los resultados
    for row in rows:
        print(row)

def init_db(conn):

    cur = conn.cursor()
    # activar PostGIS


    # --- Coordenadas de origen y destino ---
    origin_coords = (-122.4782551, 37.8199286)  # (lon, lat)
    dest_coords = (-122.4120372, 37.7803603)

    # Radio de búsqueda aproximado en grados (~1 km ≈ 0.01)
    search_radius = 10  

    # --- 1. Buscar paradas cercanas al origen ---
    origin_stops = pd.read_sql(f"""
    SELECT s.stop_id, s.stop_name
    FROM stops s
    WHERE ST_DWithin(
        ST_Transform(s.geom, 3857), -- transforma a metros
        ST_Transform(ST_SetSRID(ST_Point(-122.4782551, 37.8199286), 4326), 3857),
        500  -- metros exactos
    )
    """, conn)

    if origin_stops.empty:
        print("❌ No se encontraron paradas cercanas al ORIGEN dentro del radio especificado.")
        conn.close()
        exit()

    # --- 2. Buscar paradas cercanas al destino ---
    dest_stops = pd.read_sql(f"""
    SELECT s.stop_id, s.stop_name
    FROM stops s
    WHERE ST_DWithin(
        ST_Transform(s.geom, 3857),  -- convierte las paradas a SRID métrico
        ST_Transform(ST_SetSRID(ST_Point({dest_coords[0]}, {dest_coords[1]}), 4326), 3857),  -- punto de destino
        {search_radius_meters}  -- radio en metros, por ejemplo 500
    )
    """, conn)

    if dest_stops.empty:
        print("❌ No se encontraron paradas cercanas al DESTINO dentro del radio especificado.")
        conn.close()
        exit()

    # --- 3. Traer trips que pasan por paradas de origen ---
    origin_ids = tuple(origin_stops['stop_id'].tolist())
    origin_trips = pd.read_sql(
        "SELECT st.trip_id, st.stop_sequence, st.stop_id FROM stop_times st WHERE st.stop_id IN %s",
        conn,
        params=(origin_ids,)
    )

    # --- 4. Traer trips que pasan por paradas de destino ---
    dest_ids = tuple(dest_stops['stop_id'].tolist())
    dest_trips = pd.read_sql(
        "SELECT st.trip_id, st.stop_sequence, st.stop_id FROM stop_times st WHERE st.stop_id IN %s",
        conn,
        params=(dest_ids,)
    )

    conn.close()

    # --- 5. Merge en pandas para encontrar combinaciones válidas ---
    df = origin_trips.merge(dest_trips, on='trip_id', suffixes=('_origin', '_dest'))
    df = df[df['stop_sequence_dest'] > df['stop_sequence_origin']]

    # --- 6. Agregar nombres de paradas ---
    df = df.merge(origin_stops[['stop_id', 'stop_name']], left_on='stop_id_origin', right_on='stop_id')
    df = df.merge(dest_stops[['stop_id', 'stop_name']], left_on='stop_id_dest', right_on='stop_id', suffixes=('_origin', '_dest'))

    # --- 7. Selección de columnas finales ---
    df_final = df[['trip_id', 'stop_name_origin', 'stop_name_dest', 'stop_sequence_origin', 'stop_sequence_dest']]

    if df_final.empty:
        print("⚠️ No se encontraron viajes que conecten las paradas cercanas al origen y destino.")
    else:
        print("✅ Viajes encontrados:")
        print(df_final.head(20))
    




def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()