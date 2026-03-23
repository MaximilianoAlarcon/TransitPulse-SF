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
                    #print("✅ Transporte que lleva al destino más rápido desde ahora:")
                    #print(df_fastest.head())
                    trip_details = df_fastest.iloc[0]
                    transport_details = pd.read_sql("SELECT * FROM routes WHERE route_id IN (SELECT route_id FROM trips WHERE trip_id = %s AND operator_id = %s);",conn,params=(df_fastest['trip_id'].iloc[0], df_fastest['operator_id_origin'].iloc[0]))
                    #print("Detalles del transporte")
                    #print(transport_details.head())
                    #print(transport_details.shape)
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



MAX_WAIT_FOR_FIRST_BUS = 3600
MAX_WAIT_FOR_SECOND_BUS = 3600
 
def find_trip_with_transfer(origin_coords, dest_coords, search_radius_origin=800, search_radius_dest=1200, transfer_radius=1000, auto_estimate_radius=False):

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
    now_text = now_sf.strftime("%d/%m/%Y %H:%M:%S")
    current_sec = now_sf.hour * 3600 + now_sf.minute * 60 + now_sf.second
    max_time = current_sec + 10800

    # --- 1. stops cercanos ---
    origin_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s),4326)::geography,
            %s
        )
    """, conn, params=(origin_coords[0], origin_coords[1], search_radius_origin))

    dest_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s),4326)::geography,
            %s
        )
    """, conn, params=(dest_coords[0], dest_coords[1], search_radius_dest))

    if origin_stops.empty or dest_stops.empty:
        conn.close()
        return {"status": "Not found", "reason": "No nearby stops"}

    origin_ids = set(origin_stops["stop_id"])
    dest_ids = set(dest_stops["stop_id"])

    # --- 2. connections ---
    cur.execute("""
        SELECT from_stop, to_stop, departure_sec, arrival_sec, trip_id
        FROM connections
        WHERE departure_sec >= %s AND departure_sec <= %s
        ORDER BY departure_sec
    """, (current_sec, max_time))

    connections = cur.fetchall()

    # --- 3. CSA ---
    earliest = {}
    prev = {}
    trip_used = {}

    for s in origin_ids:
        earliest[s] = current_sec
        trip_used[s] = None

    best_target = None

    for from_stop, to_stop, dep, arr, trip in connections:

        if from_stop not in earliest:
            continue

        if dep < earliest[from_stop]:
            continue

        prev_trip = trip_used[from_stop]
        transfers = 0 if prev_trip is None else (1 if prev_trip != trip else 0)

        if transfers > 1:
            continue

        if to_stop not in earliest or arr < earliest[to_stop]:
            earliest[to_stop] = arr
            trip_used[to_stop] = trip
            prev[to_stop] = (from_stop, trip)

            if to_stop in dest_ids:
                best_target = to_stop
                break

    if not best_target:
        conn.close()
        return {"status": "Not found", "reason": "No trips with transfer in the next 3 hours"}

    # --- 4. reconstruir path ---
    path = []
    cur_stop = best_target

    while cur_stop in prev:
        p = prev[cur_stop]
        path.append((p[0], cur_stop, p[1]))
        cur_stop = p[0]

    path.reverse()

    # separar legs
    legs = []
    current_leg = [path[0]]
    current_trip = path[0][2]

    for step in path[1:]:
        if step[2] == current_trip:
            current_leg.append(step)
        else:
            legs.append((current_trip, current_leg))
            current_leg = [step]
            current_trip = step[2]

    legs.append((current_trip, current_leg))

    if len(legs) < 2:
        conn.close()
        return {"status": "Not found", "reason": "No transfer route found"}

    leg1, leg2 = legs[0], legs[1]

    # --- 5. stops ---
    origin_stop_id = leg1[1][0][0]
    transfer_stop_id = leg1[1][-1][1]
    dest_stop_id = leg2[1][-1][1]

    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE stop_id = ANY(%s)
    """, ([origin_stop_id, transfer_stop_id, dest_stop_id],))

    stops_map = {row[0]: row for row in cur.fetchall()}

    # --- 6. transport (🔥 FIX CLAVE) ---
    trip_ids = [leg1[0], leg2[0]]

    cur.execute("""
        SELECT 
            t.trip_id,
            t.operator_id,
            t.route_id,
            r.route_type,
            r.route_color,
            r.route_short_name,
            r.route_long_name
        FROM trips t
        JOIN routes r ON t.route_id = r.route_id AND t.operator_id = r.operator_id
        WHERE t.trip_id = ANY(%s)
    """, (trip_ids,))

    transport_map = {
        row[0]: {
            "trip_id": row[0],
            "operator_id": row[1],
            "route_id": row[2],
            "route_type": row[3],
            "route_color": row[4],
            "route_short_name": row[5],
            "route_long_name": row[6],
        }
        for row in cur.fetchall()
    }

    origin_stop = stops_map[origin_stop_id]
    transfer_stop = stops_map[transfer_stop_id]
    dest_stop = stops_map[dest_stop_id]

    # --- 7. legs ---
    leg1_trip_details = {
        "trip_id": leg1[0],
        "operator_id_origin": transport_map[leg1[0]]["operator_id"],
        "route_type": transport_map[leg1[0]]["route_type"],
        "route_color": transport_map[leg1[0]]["route_color"],
        "route_short_name": transport_map[leg1[0]]["route_short_name"],
        "route_long_name": transport_map[leg1[0]]["route_long_name"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest": len(leg1[1]),
        "stop_lat_origin": origin_stop[2],
        "stop_lon_origin": origin_stop[3],
        "stop_lat_dest": transfer_stop[2],
        "stop_lon_dest": transfer_stop[3],
        "stop_name_origin": origin_stop[1]
    }

    leg2_trip_details = {
        "trip_id": leg2[0],
        "operator_id_origin": transport_map[leg2[0]]["operator_id"],
        "route_type": transport_map[leg2[0]]["route_type"],
        "route_color": transport_map[leg2[0]]["route_color"],
        "route_short_name": transport_map[leg2[0]]["route_short_name"],
        "route_long_name": transport_map[leg2[0]]["route_long_name"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest": len(leg2[1]),
        "stop_lat_origin": transfer_stop[2],
        "stop_lon_origin": transfer_stop[3],
        "stop_name_origin": transfer_stop[1],
        "stop_lat_dest": dest_stop[2],
        "stop_lon_dest": dest_stop[3]
    }

    leg1_geom = get_direct_trip_geometry(cur, leg1_trip_details, transport_map[leg1[0]], search_shapes=True)
    leg2_geom = get_direct_trip_geometry(cur, leg2_trip_details, transport_map[leg2[0]], search_shapes=True)

    total_time = earliest[dest_stop_id] - current_sec

    conn.close()

    return {
        "status": "Found",
        "details": [{
            "origin": {"id": origin_stop[0], "name": origin_stop[1], "lat": origin_stop[2], "lon": origin_stop[3]},
            "transfer": {"id": transfer_stop[0], "name": transfer_stop[1], "lat": transfer_stop[2], "lon": transfer_stop[3]},
            "destination": {"id": dest_stop[0], "name": dest_stop[1], "lat": dest_stop[2], "lon": dest_stop[3]},
            "leg1": {"trip_details": leg1_trip_details, "transport_details": transport_map[leg1[0]], "trip_geometry": leg1_geom},
            "leg2": {"trip_details": leg2_trip_details, "transport_details": transport_map[leg2[0]], "trip_geometry": leg2_geom},
            "total_time": total_time,
            "wait_time": 0,
            "now_time": now_text
        }]
    }