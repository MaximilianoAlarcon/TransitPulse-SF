import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from utils import get_direct_trip_geometry,estimate_radius,should_use_transit
import math

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

pd.set_option('display.max_columns', None)  # mostrar todas las columnas
pd.set_option('display.width', 200)         # ancho de la tabla en consola
pd.set_option('display.max_rows', 50)      # mostrar hasta 50 filas

WAIT_TRANSPORT_LIMIT = 1000000

def find_direct_trip(origin_coords, dest_coords, search_radius_origin=800, search_radius_dest=800, auto_estimate_radius=False):

    if should_use_transit(origin_coords,dest_coords):

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        if auto_estimate_radius:
            search_radius_origin =  estimate_radius(conn,origin_coords)
            search_radius_dest =  estimate_radius(conn,dest_coords)

        print("Radio para origen: "+str(search_radius_origin))
        print("Radio para destino: "+str(search_radius_dest))

        # activar PostGIS

        # --- 1. Buscar paradas cercanas al origen ---
        origin_stops = pd.read_sql(f"""
        SELECT s.stop_lat, s.stop_lon, s.stop_id, s.stop_name
        FROM stops s
        WHERE ST_DWithin(
            s.geom::geography,
            ST_SetSRID(ST_Point({origin_coords[0]}, {origin_coords[1]}), 4326)::geography,
            {search_radius_origin}
        )
        """, conn)

        if origin_stops.empty:
            print("❌ No se encontraron paradas cercanas al ORIGEN dentro del radio especificado.")
            return {
                "status":"Not found",
                "reason":"There is no stop near the origin"
            }

        # --- 2. Buscar paradas cercanas al destino ---
        dest_stops = pd.read_sql(f"""
        SELECT s.stop_lat, s.stop_lon, s.stop_id, s.stop_name
        FROM stops s
        WHERE ST_DWithin(
            s.geom::geography,
            ST_SetSRID(ST_Point({dest_coords[0]}, {dest_coords[1]}), 4326)::geography,
            {search_radius_dest}
        )
        """, conn)

        if dest_stops.empty:
            print("❌ No se encontraron paradas cercanas al DESTINO dentro del radio especificado.")
            return {
                "status":"Not found",
                "reason":"There is no stop near the destination."
            }

        # --- 3. Traer trips que pasan por paradas de origen ---
        origin_ids = tuple(origin_stops['stop_id'].tolist())
        print("Origin ids")
        print(origin_ids)

        dest_ids = tuple(dest_stops['stop_id'].tolist())
        print("Destination ids")
        print(dest_ids)

        if len(origin_ids) > 0 and len(dest_ids) > 0:

            now_sf = datetime.now(ZoneInfo("America/Los_Angeles")) 
            current_sec = now_sf.hour*3600 + now_sf.minute*60 + now_sf.second
            arrival_end = current_sec + WAIT_TRANSPORT_LIMIT

            print("Ejecutando query 3")
            origin_trips = pd.read_sql(
                """
                SELECT st.operator_id, st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time, st.arrival_sec 
                FROM stop_times st 
                WHERE st.stop_id IN %s
                AND st.arrival_sec IS NOT NULL
                AND st.arrival_sec BETWEEN %s AND %s
                """,
                conn,
                params=(origin_ids,current_sec,arrival_end)
            )

            # --- 4. Traer trips que pasan por paradas de destino ---
            print("Ejecutando query 4")
            dest_trips = pd.read_sql(
                """
                SELECT st.operator_id, st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time, st.arrival_sec 
                FROM stop_times st 
                WHERE st.stop_id IN %s
                AND st.arrival_sec IS NOT NULL
                AND st.arrival_sec BETWEEN %s AND %s
                """,
                conn,
                params=(dest_ids,current_sec,arrival_end)
            )

            # --- Bloque completo para combinar trips y agregar nombres de paradas ---
            # 1. Merge de trips por trip_id
            df = origin_trips.merge(dest_trips, on='trip_id', suffixes=('_origin', '_dest'))

            # 2. Filtrar secuencias válidas (destino después del origen)
            df = df[df['stop_sequence_dest'] > df['stop_sequence_origin']]

            # 3. Renombrar columnas de stops para evitar conflictos al merge
            origin_stops_renamed = origin_stops.rename(columns={
                'operator_id':'operator_id_origin',
                'stop_id': 'stop_id_origin',
                'stop_name': 'stop_name_origin',
                'arrival_time': 'arrival_time_origin',
                'stop_lat': 'stop_lat_origin',
                'stop_lon': 'stop_lon_origin'
            })
            dest_stops_renamed = dest_stops.rename(columns={
                'operator_id':'operator_id_dest',
                'stop_id': 'stop_id_dest',
                'stop_name': 'stop_name_dest',
                'arrival_time': 'arrival_time_dest',
                'stop_lat': 'stop_lat_dest',
                'stop_lon': 'stop_lon_dest'
            })

            # 4. Merge para agregar nombres de paradas
            df = df.merge(origin_stops_renamed, on='stop_id_origin')
            df = df.merge(dest_stops_renamed, on='stop_id_dest')

            # 5. Selección de columnas finales
            df_final = df[['trip_id', 'stop_name_origin', 'arrival_time_origin', 'stop_name_dest', 
            'arrival_time_dest', 'stop_sequence_origin', 'stop_sequence_dest', 'operator_id_origin', 
            'operator_id_dest','stop_lat_origin','stop_lon_origin','stop_lat_dest','stop_lon_dest']]
            df_final = df_final.drop_duplicates(subset=['trip_id', 'stop_sequence_origin', 'stop_sequence_dest'])
            print("Tamaño del dataframe final")
            print(df_final.shape)

            if df_final.shape[0] > 0:
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
                if df_fastest.shape[0] > 0:
                    print("✅ Bus que te lleva al destino más rápido desde ahora:")
                    print(df_fastest.head())
                    trip_details = df_fastest.iloc[0]
                    transport_details = pd.read_sql("SELECT * FROM routes WHERE route_id IN (SELECT route_id FROM trips WHERE trip_id = %s AND operator_id = %s);",conn,params=(df_fastest['trip_id'].iloc[0], df_fastest['operator_id_origin'].iloc[0]))
                    print("Detalles del transporte")
                    print(transport_details.head())
                    print(transport_details.shape)
                    transport_details = transport_details.iloc[0]
                    t1 = trip_details["arrival_time_origin"]
                    t2 = trip_details["arrival_time_dest"]
                    print("Buscando los shapes de la ruta")
                    trip_geometry = get_direct_trip_geometry(cur, trip_details, transport_details)
                    print(trip_geometry)
                    return {
                        "status":"Found",
                        "details":{
                            "stop_name_origin":trip_details["stop_name_origin"],
                            "arrival_time_origin":f"{t1.components.hours:02}:{t1.components.minutes:02}",
                            "stop_name_dest":trip_details["stop_name_dest"],
                            "arrival_time_dest":f"{t2.components.hours:02}:{t2.components.minutes:02}",
                            "stop_lat_origin":float(trip_details["stop_lat_origin"]),
                            "stop_lon_origin":float(trip_details["stop_lon_origin"]),
                            "stop_lat_dest":float(trip_details["stop_lat_dest"]),
                            "stop_lon_dest":float(trip_details["stop_lon_dest"]),
                            "travel_time":int(trip_details["travel_time"].total_seconds()),
                            "wait_time":int(trip_details["wait_time"].total_seconds()),
                            "total_time":int(trip_details["total_time"].total_seconds()),
                            "route_short_name":transport_details["route_short_name"],
                            "route_long_name":transport_details["route_long_name"],
                            "route_desc":transport_details["route_desc"],
                            "route_type":int(transport_details["route_type"]),
                            "route_url":transport_details["route_url"],
                            "route_color":transport_details["route_color"],
                            "route_type":transport_details["route_type"],
                            "trip_geometry":trip_geometry
                        }
                    }
                else:
                    print("⚠️ No se encontraron viajes directos porque no hay paradas cercanas al origen o destino.")
                    return {
                        "status":"Not found",
                        "reason":"No direct trips were found between the origin and destination"
                    }
            else:
                print("⚠️ No se encontraron viajes directos porque no hay paradas cercanas al origen o destino.")
                return {
                    "status":"Not found",
                    "reason":"No direct trips were found between the origin and destination"
                }
        else:
            print("⚠️ No se encontraron viajes directos porque no hay paradas cercanas al origen o destino.")
            return {
                "status":"Not found",
                "reason":"No direct trips were found between the origin and destination"
            }
    else:
        return {
                "status":"Canceled",
                "reason":"You should go walking"
            }