import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from utils import get_direct_trip_geometry,estimate_radius,should_use_transit,time_to_seconds,estimate_radius_and_limit
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

WAIT_TRANSPORT_LIMIT = 3600

def find_direct_trip(origin_coords, dest_coords, search_radius_origin=800, search_radius_dest=800, auto_estimate_radius=False):

    if should_use_transit(origin_coords,dest_coords):

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        if auto_estimate_radius:
            search_radius_origin =  estimate_radius(conn,origin_coords)
            search_radius_dest =  estimate_radius(conn,dest_coords)

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
            return {
                "status":"Not found",
                "reason":"There is no stop near the destination."
            }

        # --- 3. Traer trips que pasan por paradas de origen ---
        origin_ids = tuple(origin_stops['stop_id'].tolist())
        dest_ids = tuple(dest_stops['stop_id'].tolist())

        if len(origin_ids) > 0 and len(dest_ids) > 0:

            now_sf = datetime.now(ZoneInfo("America/Los_Angeles")) 
            current_sec = now_sf.hour*3600 + now_sf.minute*60 + now_sf.second
            arrival_end = current_sec + WAIT_TRANSPORT_LIMIT

            origin_placeholders = ','.join(['%s'] * len(origin_ids))
            dest_placeholders = ','.join(['%s'] * len(dest_ids))

            origin_trips = pd.read_sql(
                f"""
                SELECT st.operator_id, st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time, st.arrival_sec 
                FROM stop_times st 
                WHERE st.stop_id IN ({origin_placeholders}) AND st.arrival_sec IS NOT NULL
                """,
                conn,
                params=(origin_ids)
            )

            # --- 4. Traer trips que pasan por paradas de destino ---

            dest_trips = pd.read_sql(
                f"""
                SELECT st.operator_id, st.trip_id, st.stop_sequence, st.stop_id, st.arrival_time, st.arrival_sec 
                FROM stop_times st 
                WHERE st.stop_id IN ({dest_placeholders}) AND st.arrival_sec IS NOT NULL
                """,
                conn,
                params=(dest_ids)
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

            if df_final.shape[0] > 0:
                # Hora actual como timedelta
                
                #current_time = datetime.now().strftime("%H:%M:%S")
                #now = pd.to_timedelta(current_time)
                
                now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
                now = pd.to_timedelta(now_sf.hour, unit='h') + pd.to_timedelta(now_sf.minute, unit='m') + pd.to_timedelta(now_sf.second, unit='s')
            
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
                    print("✅ Transporte que lleva al destino más rápido desde ahora:")
                    print(df_fastest.head())
                    trip_details = df_fastest.iloc[0]
                    transport_details = pd.read_sql("SELECT * FROM routes WHERE route_id IN (SELECT route_id FROM trips WHERE trip_id = %s AND operator_id = %s);",conn,params=(df_fastest['trip_id'].iloc[0], df_fastest['operator_id_origin'].iloc[0]))
                    print("Detalles del transporte")
                    print(transport_details.head())
                    print(transport_details.shape)
                    transport_details = transport_details.iloc[0]
                    t1 = trip_details["arrival_time_origin"]
                    t2 = trip_details["arrival_time_dest"]
                    trip_geometry = get_direct_trip_geometry(cur, trip_details, transport_details,search_shapes=True)
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
                    return {
                        "status":"Not found",
                        "reason":"No direct trips were found between the origin and destination"
                    }
            else:
                return {
                    "status":"Not found",
                    "reason":"No direct trips were found between the origin and destination"
                }
        else:
            return {
                "status":"Not found",
                "reason":"No direct trips were found between the origin and destination"
            }
    else:
        return {
                "status":"Canceled",
                "reason":"You should go walking"
            }



MAX_WAIT_FOR_FIRST_BUS = 18000
MAX_WAIT_FOR_SECOND_BUS = 18000
 
def find_trip_with_transfer(origin_coords, dest_coords, search_radius_origin=800, search_radius_dest=1200, transfer_radius=1000, auto_estimate_radius=False):
 
    conn = psycopg2.connect(**DB_CONFIG)
 
    limit_stops_origin = 20
    limit_stops_dest = 20
    if auto_estimate_radius:
        search_radius_origin,limit_stops_origin = estimate_radius_and_limit(conn, origin_coords)
        search_radius_dest,limit_stops_dest = estimate_radius_and_limit(conn, dest_coords)
 
    now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
    now_text = now_sf.strftime("%d/%m/%Y %H:%M:%S")
    current_sec = now_sf.hour * 3600 + now_sf.minute * 60 + now_sf.second
 
    print("search_radius_origin -> "+str(search_radius_origin)+" limit_stops_origin -> "+str(limit_stops_origin))
    print("search_radius_dest -> "+str(search_radius_dest)+" limit_stops_dest -> "+str(limit_stops_dest))
 
    query = f"""
    WITH origin AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_Point(%s, %s),4326)::geography, %s)
        ORDER BY ST_Distance(geom::geography, ST_SetSRID(ST_Point(%s, %s),4326)::geography)
        --LIMIT {limit_stops_origin}
    ),
    dest AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_Point(%s, %s),4326)::geography, %s)
        ORDER BY ST_Distance(geom::geography, ST_SetSRID(ST_Point(%s, %s),4326)::geography)
        --LIMIT {limit_stops_dest}
    ),
    first_leg AS (
        SELECT st.*
        FROM stop_times st
        JOIN stops s ON st.stop_id = s.stop_id
        WHERE st.stop_id IN (SELECT stop_id FROM origin)
          AND st.arrival_sec IS NOT NULL
          AND st.departure_sec BETWEEN %s AND %s + {MAX_WAIT_FOR_FIRST_BUS}
        ORDER BY st.departure_sec
        LIMIT 100
    ),
    transfers AS (
        SELECT
            st1.trip_id AS trip1,
            st2.trip_id AS trip2,
            st1.stop_id AS leg1_stop,
            st2.stop_id AS leg2_stop,
            st1.arrival_sec AS t1,
            st2.departure_sec AS t2,
            st1.stop_sequence AS seq1,
            st2.stop_sequence AS seq2,
            st2.arrival_time AS arrival_time_second_trip
        FROM first_leg st1
        JOIN stop_times st2
          ON st2.departure_sec > st1.arrival_sec
         AND st2.departure_sec < st1.arrival_sec + {MAX_WAIT_FOR_SECOND_BUS}
        JOIN stops s1 ON st1.stop_id = s1.stop_id
        JOIN stops s2 ON st2.stop_id = s2.stop_id
        WHERE ST_DWithin(s1.geom::geography, s2.geom::geography, {transfer_radius})
    ),
    final_routes AS (
        SELECT
            t.trip1,
            t.trip2,
            t.leg1_stop,
            t.leg2_stop,
            t.t1,
            t.t2,
            t.seq1,
            t.seq2,
            t.arrival_time_second_trip,
            st3.arrival_sec AS dest_time,
            st3.arrival_time AS dest_arrival_time,
            st3.stop_id AS dest_stop,
            st3.stop_sequence AS seq3
        FROM transfers t
        JOIN stop_times st3 ON t.trip2 = st3.trip_id
        WHERE st3.stop_id IN (SELECT stop_id FROM dest)
          AND st3.stop_sequence > t.seq2
        ORDER BY st3.arrival_sec
        LIMIT 20
    )
    SELECT *,
           (dest_time - %s) %% 86400 AS total_travel_time,
           (t1 - %s) AS wait_for_first_bus
    FROM final_routes
    ORDER BY total_travel_time
    LIMIT 10;
    """
 
    print(query)
 
    params = (
        origin_coords[0], origin_coords[1], search_radius_origin, origin_coords[0], origin_coords[1],
        dest_coords[0], dest_coords[1], search_radius_dest, dest_coords[0], dest_coords[1],
        current_sec, current_sec,
        current_sec, current_sec
    )
 
    df = pd.read_sql(query, conn, params=params)
 
    if df.empty:
        conn.close()
        return {"status": "Not found", "reason": "No trips with transfer in the next hour"}
 
    # Filtrado de filas válidas
    df = df[(df["total_travel_time"] > 0) & (df["wait_for_first_bus"] >= 0)]
    df = df.drop_duplicates(subset=["leg2_stop", "dest_stop"], keep="first").head(1)
 
    if df.empty:
        conn.close()
        return {"status": "Not found", "reason": "All trips already departed"}
 
    # Recuperar stops y trips en lote
    cur = conn.cursor()
    all_stop_ids = set(df["leg1_stop"]).union(df["leg2_stop"]).union(df["dest_stop"])
    cur.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops WHERE stop_id = ANY(%s);",
                (list(all_stop_ids),))
    stops_map = {row[0]: row for row in cur.fetchall()}
 
    trip_ids = list(set(df["trip1"]).union(df["trip2"]))
    cur.execute("""
        SELECT t.trip_id, t.operator_id, r.route_id, r.route_type, r.route_color, r.route_short_name, r.route_long_name
        FROM trips t
        JOIN routes r ON t.route_id = r.route_id AND t.operator_id = r.operator_id
        WHERE t.trip_id = ANY(%s)
    """, (trip_ids,))
    transport_map = {row[0]: {"operator_id": row[1], "route_id": row[2], "route_type": row[3],
                              "route_color": row[4], "route_short_name": row[5], "route_long_name": row[6]}
                     for row in cur.fetchall()}
 
    routes = []
    for _, row in df.iterrows():
        origin_stop = stops_map[row["leg1_stop"]]    # Punto 1: Trip 1 Origin
        transfer_stop = stops_map[row["leg2_stop"]]  # Punto 2/3: Trip 1 Dest = Trip 2 Origin
        dest_stop = stops_map[row["dest_stop"]]      # Punto 4: Trip 2 Dest
 
        # FIX: usar row["leg1_stop"] en lugar de list(stops_map.keys())[0]
        cur.execute(
            "SELECT stop_sequence FROM stop_times WHERE trip_id=%s AND stop_id=%s LIMIT 1;",
            (row["trip1"], row["leg1_stop"])
        )
        origin_seq_row = cur.fetchone()
        origin_seq = origin_seq_row[0] if origin_seq_row else 0
 
        leg1_trip_details = {
            "trip_id": row["trip1"],
            "operator_id_origin": transport_map[row["trip1"]]["operator_id"],
            "route_type": transport_map[row["trip1"]]["route_type"],
            "route_long_name": transport_map[row["trip1"]]["route_long_name"],
            "route_short_name": transport_map[row["trip1"]]["route_short_name"],
            "route_color": transport_map[row["trip1"]]["route_color"],
            "stop_sequence_origin": origin_seq,        # secuencia del Punto 1
            "stop_sequence_dest": row["seq1"],         # secuencia del Punto 2
            "wait_for_first_bus": row["wait_for_first_bus"],
            # Punto 1: Trip 1 Origin
            "stop_lat_origin": origin_stop[2],
            "stop_lon_origin": origin_stop[3],
            # Punto 2: Trip 1 Dest (= punto de trasbordo)
            "stop_lat_dest": transfer_stop[2],
            "stop_lon_dest": transfer_stop[3]
        }
 
        leg2_trip_details = {
            "trip_id": row["trip2"],
            "operator_id_origin": transport_map[row["trip2"]]["operator_id"],
            "route_type": transport_map[row["trip2"]]["route_type"],
            "route_long_name": transport_map[row["trip2"]]["route_long_name"],
            "route_short_name": transport_map[row["trip2"]]["route_short_name"],
            "route_color": transport_map[row["trip2"]]["route_color"],
            "stop_sequence_origin": row["seq2"],       # secuencia del Punto 3
            "stop_sequence_dest": row["seq3"],         # secuencia del Punto 4
            "arrival_time_second_trip": row["arrival_time_second_trip"],
            "dest_arrival_time": row["dest_arrival_time"],
            # Punto 3: Trip 2 Origin (= punto de trasbordo)
            "stop_lat_origin": transfer_stop[2],
            "stop_lon_origin": transfer_stop[3],
            "stop_name_origin": transfer_stop[1],
            # Punto 4: Trip 2 Dest
            "stop_lat_dest": dest_stop[2],
            "stop_lon_dest": dest_stop[3]
        }
 
        # geometría del viaje
        leg1_trip_geometry = get_direct_trip_geometry(cur, leg1_trip_details, transport_map[row["trip1"]],search_shapes=True)
        leg2_trip_geometry = get_direct_trip_geometry(cur, leg2_trip_details, transport_map[row["trip2"]],search_shapes=True)
 
        routes.append({
            # FIX: origin completo con los datos del Punto 1
            "origin": {
                "id": origin_stop[0],
                "name": origin_stop[1],
                "lat": origin_stop[2],
                "lon": origin_stop[3]
            },
            "transfer": {"id": transfer_stop[0], "name": transfer_stop[1], "lat": transfer_stop[2], "lon": transfer_stop[3]},
            "destination": {"id": dest_stop[0], "name": dest_stop[1], "lat": dest_stop[2], "lon": dest_stop[3]},
            "leg1": {"trip_details": leg1_trip_details, "transport_details": transport_map[row["trip1"]], "trip_geometry": leg1_trip_geometry},
            "leg2": {"trip_details": leg2_trip_details, "transport_details": transport_map[row["trip2"]], "trip_geometry": leg2_trip_geometry},
            "total_time": row["total_travel_time"],
            "wait_time": row["t2"] - row["t1"],
            "now_time": now_text
        })
 
    cur.close()
    conn.close()
    return {"status": "Found", "details": routes}