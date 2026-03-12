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


    # 1. Origen
    origin_stops = pd.read_sql("""
    SELECT s.stop_id, s.stop_name, s.geom
    FROM stops s
    WHERE s.geom <-> ST_SetSRID(ST_Point(-122.4782551, 37.8199286), 4326) < 0.005
    """, conn)

    # 2. Destino
    dest_stops = pd.read_sql("""
    SELECT s.stop_id, s.stop_name, s.geom
    FROM stops s
    WHERE s.geom <-> ST_SetSRID(ST_Point(-122.4120372, 37.7803603), 4326) < 0.005
    """, conn)

    # 3. Trips por origen
    origin_trips = pd.read_sql(f"""
    SELECT st.trip_id, st.stop_sequence, st.stop_id
    FROM stop_times st
    WHERE st.stop_id IN ({','.join([str(x) for x in origin_stops.stop_id])})
    """, conn)

    # 4. Trips por destino
    dest_trips = pd.read_sql(f"""
    SELECT st.trip_id, st.stop_sequence, st.stop_id
    FROM stop_times st
    WHERE st.stop_id IN ({','.join([str(x) for x in dest_stops.stop_id])})
    """, conn)

    # 5. Merge en pandas
    df = origin_trips.merge(dest_trips, on='trip_id', suffixes=('_origin', '_dest'))
    df = df[df['stop_sequence_dest'] > df['stop_sequence_origin']]

    # 6. Agregar nombres de stops
    df = df.merge(origin_stops[['stop_id', 'stop_name']], left_on='stop_id_origin', right_on='stop_id')
    df = df.merge(dest_stops[['stop_id', 'stop_name']], left_on='stop_id_dest', right_on='stop_id', suffixes=('_origin', '_dest'))

    # Resultado final
    df = df[['trip_id', 'stop_name_origin', 'stop_name_dest', 'stop_sequence_origin', 'stop_sequence_dest']]

    print(df.head(20))
    




def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()