import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

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

pd.set_option('display.max_columns', None)  # mostrar todas las columnas
pd.set_option('display.width', 200)         # ancho de la tabla en consola
pd.set_option('display.max_rows', 50)      # mostrar hasta 50 filas


def init_db(conn):

    cur = conn.cursor()
    # activar PostGIS


    # --- Coordenadas de origen y destino ---
    origin_coords = (-122.3816274 , 37.61911449999999)  # (lon, lat)
    dest_coords = (-122.4120372 , 37.7803603)

    # Radio de búsqueda aproximado en grados (~1 km ≈ 0.01)
    search_radius = 500 

    # --- 1. Buscar paradas cercanas al origen ---
    origin_stops = pd.read_sql(f"""
    SELECT s.stop_id, s.stop_name
    FROM stops s
    WHERE ST_DWithin(
        s.geom::geography,
        ST_SetSRID(ST_Point({origin_coords[0]}, {origin_coords[1]}), 4326)::geography,
        {search_radius}
    )
    """, conn)

    if origin_stops.empty:
        print("❌ No se encontraron paradas cercanas al ORIGEN dentro del radio especificado.")


    # --- 2. Buscar paradas cercanas al destino ---
    dest_stops = pd.read_sql(f"""
    SELECT s.stop_id, s.stop_name
    FROM stops s
    WHERE ST_DWithin(
        s.geom::geography,
        ST_SetSRID(ST_Point({dest_coords[0]}, {dest_coords[1]}), 4326)::geography,
        {search_radius}
    )
    """, conn)

    if dest_stops.empty:
        print("❌ No se encontraron paradas cercanas al DESTINO dentro del radio especificado.")


    # --- 3. Traer trips que pasan por paradas de origen ---
    origin_ids = tuple(origin_stops['stop_id'].tolist())
    print("Origin ids")
    print(origin_ids)


    dest_ids = tuple(dest_stops['stop_id'].tolist())
    print("Destination ids")
    print(dest_ids)

    if len(origin_ids) > 0 and len(dest_ids) > 0:

        print("Ejecutando query 3")
        origin_trips = pd.read_sql(
            "SELECT st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time FROM stop_times st WHERE st.stop_id IN %s",
            conn,
            params=(origin_ids,)
        )

        # --- 4. Traer trips que pasan por paradas de destino ---
        print("Ejecutando query 4")
        dest_trips = pd.read_sql(
            "SELECT st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time FROM stop_times st WHERE st.stop_id IN %s",
            conn,
            params=(dest_ids,)
        )


        # --- Bloque completo para combinar trips y agregar nombres de paradas ---
        # 1. Merge de trips por trip_id
        df = origin_trips.merge(dest_trips, on='trip_id', suffixes=('_origin', '_dest'))

        # 2. Filtrar secuencias válidas (destino después del origen)
        df = df[df['stop_sequence_dest'] > df['stop_sequence_origin']]

        # 3. Renombrar columnas de stops para evitar conflictos al merge
        origin_stops_renamed = origin_stops.rename(columns={
            'stop_id': 'stop_id_origin',
            'stop_name': 'stop_name_origin',
            'arrival_time': 'arrival_time_origin'
        })
        dest_stops_renamed = dest_stops.rename(columns={
            'stop_id': 'stop_id_dest',
            'stop_name': 'stop_name_dest',
            'arrival_time': 'arrival_time_dest'
        })

        # 4. Merge para agregar nombres de paradas
        df = df.merge(origin_stops_renamed, on='stop_id_origin')
        df = df.merge(dest_stops_renamed, on='stop_id_dest')

        # 5. Selección de columnas finales


        df_final = df[['trip_id', 'stop_name_origin', 'arrival_time_origin', 'stop_name_dest', 'arrival_time_dest', 'stop_sequence_origin', 'stop_sequence_dest']]
        df_final = df_final.drop_duplicates(subset=['trip_id', 'stop_sequence_origin', 'stop_sequence_dest'])
        print("Tamaño del dataframe final")
        print(df_final.shape)


        # Hora actual como timedelta
        current_time = datetime.now().strftime("%H:%M:%S")
        now = pd.to_timedelta(current_time)

        # Convertir arrival_time a timedelta
        df_final['arrival_time_origin'] = pd.to_timedelta(df_final['arrival_time_origin'])
        df_final['arrival_time_dest'] = pd.to_timedelta(df_final['arrival_time_dest'])

        # Calcular travel_time
        df_final['travel_time'] = df_final['arrival_time_dest'] - df_final['arrival_time_origin']

        # Calcular wait_time y tiempo totalW
        df_final['wait_time'] = df_final['arrival_time_origin'] - now
        df_final = df_final[df_final['wait_time'] >= pd.Timedelta(0)]  # descartar buses que ya pasaron
        df_final['total_time'] = df_final['wait_time'] + df_final['travel_time']

        # Elegir bus que llega primero considerando espera
        df_fastest = df_final.sort_values('total_time').head(1)

        print("✅ Bus que te lleva al destino más rápido desde ahora:")
        print(df_fastest[['trip_id', 'stop_name_origin', 'stop_name_dest', 'wait_time', 'travel_time', 'total_time']])


        # 6. Mensaje según resultado
        if df_final.empty:
            print("⚠️ No se encontraron viajes que conecten las paradas cercanas al origen y destino.")
        else:
            print("✅ Viajes encontrados:")
            print(df_final.head(20))
    
    else:
        print("⚠️ No se encontraron viajes directos porque no hay paradas cercanas al origen o destino.")




def run():

    conn = psycopg2.connect(**DB_CONFIG)

    init_db(conn)

    conn.close()