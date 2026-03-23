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
    max_time = current_sec + 10800  # 3 horas

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
        return {"status": "Not found", "reason": "No nearby stops"}

    origin_ids = set(origin_stops["stop_id"])
    dest_ids = set(dest_stops["stop_id"])

    # --- 2. conexiones ---
    connections = pd.read_sql("""
        SELECT trip_id, stop_id, stop_sequence, departure_sec, arrival_sec
        FROM stop_times
        WHERE departure_sec >= %s AND departure_sec <= %s
        ORDER BY trip_id, stop_sequence
    """, conn, params=(current_sec, max_time))

    # --- 3. stops necesarios (reducir universo)
    relevant_stop_ids = set(connections["stop_id"]).union(origin_ids).union(dest_ids)

    stops_df = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon, geom
        FROM stops
        WHERE stop_id = ANY(%s)
    """, conn, params=(list(relevant_stop_ids),))

    # --- 4. PRECOMPUTAR transferencias (CLAVE 🔥)
    transfer_map = {}

    stops_list = stops_df.to_dict("records")

    for s1 in stops_list:
        s1_id = s1["stop_id"]

        nearby = stops_df[
            ((stops_df["stop_lat"] - s1["stop_lat"])**2 +
             (stops_df["stop_lon"] - s1["stop_lon"])**2) < (0.01)  # approx ~1km
        ]

        transfer_map[s1_id] = set(nearby["stop_id"])

    # --- 5. agrupar trips
    trips = {}
    for _, row in connections.iterrows():
        trips.setdefault(row["trip_id"], []).append(row)

    best_option = None

    # --- 6. LOOP SIN DB 🚀
    for trip_id, stops in trips.items():

        for i, s in enumerate(stops):
            if s["stop_id"] in origin_ids and s["departure_sec"] >= current_sec:

                for j in range(i+1, min(i+12, len(stops))):
                    transfer_stop = stops[j]
                    nearby_ids = transfer_map.get(transfer_stop["stop_id"], set())

                    # buscar segundo viaje SIN SQL
                    candidates = connections[
                        (connections["stop_id"].isin(nearby_ids)) &
                        (connections["departure_sec"] > transfer_stop["arrival_sec"])
                    ]

                    for _, st2 in candidates.iterrows():
                        trip2 = st2["trip_id"]
                        if trip2 == trip_id:
                            continue

                        trip2_stops = trips.get(trip2, [])

                        for s2 in trip2_stops:
                            if (
                                s2["stop_id"] in dest_ids and
                                s2["stop_sequence"] > st2["stop_sequence"]
                            ):
                                total_time = s2["arrival_sec"] - current_sec

                                if best_option is None or total_time < best_option["total_time"]:
                                    best_option = {
                                        "trip1": trip_id,
                                        "trip2": trip2,
                                        "origin_stop": s["stop_id"],
                                        "transfer_stop": transfer_stop["stop_id"],
                                        "dest_stop": s2["stop_id"],
                                        "seq_origin": s["stop_sequence"],
                                        "seq_transfer": transfer_stop["stop_sequence"],
                                        "seq2": st2["stop_sequence"],
                                        "seq3": s2["stop_sequence"],
                                        "total_time": total_time,
                                        "wait_time": st2["departure_sec"] - transfer_stop["arrival_sec"]
                                    }

    if not best_option:
        conn.close()
        return {"status": "Not found", "reason": "No trips with transfer in the next 3 hours"}

    # --- 7. reconstrucción ---
    all_stop_ids = [best_option["origin_stop"], best_option["transfer_stop"], best_option["dest_stop"]]
    cur.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops WHERE stop_id = ANY(%s);", (all_stop_ids,))
    stops_map = {row[0]: row for row in cur.fetchall()}

    trip_ids = [best_option["trip1"], best_option["trip2"]]
    cur.execute("""
        SELECT t.trip_id, t.operator_id, r.route_type, r.route_color, r.route_short_name, r.route_long_name
        FROM trips t
        JOIN routes r ON t.route_id = r.route_id AND t.operator_id = r.operator_id
        WHERE t.trip_id = ANY(%s)
    """, (trip_ids,))
    transport_map = {row[0]: row for row in cur.fetchall()}

    origin_stop = stops_map[best_option["origin_stop"]]
    transfer_stop = stops_map[best_option["transfer_stop"]]
    dest_stop = stops_map[best_option["dest_stop"]]

    leg1_trip_details = {
        "trip_id": best_option["trip1"],
        "operator_id_origin": transport_map[best_option["trip1"]][1],
        "route_type": transport_map[best_option["trip1"]][2],
        "route_color": transport_map[best_option["trip1"]][3],
        "route_short_name": transport_map[best_option["trip1"]][4],
        "route_long_name": transport_map[best_option["trip1"]][5],
        "stop_sequence_origin": best_option["seq_origin"],
        "stop_sequence_dest": best_option["seq_transfer"],
        "stop_lat_origin": origin_stop[2],
        "stop_lon_origin": origin_stop[3],
        "stop_lat_dest": transfer_stop[2],
        "stop_lon_dest": transfer_stop[3],
        "stop_name_origin": origin_stop[1]
    }

    leg2_trip_details = {
        "trip_id": best_option["trip2"],
        "operator_id_origin": transport_map[best_option["trip2"]][1],
        "route_type": transport_map[best_option["trip2"]][2],
        "route_color": transport_map[best_option["trip2"]][3],
        "route_short_name": transport_map[best_option["trip2"]][4],
        "route_long_name": transport_map[best_option["trip2"]][5],
        "stop_sequence_origin": best_option["seq2"],
        "stop_sequence_dest": best_option["seq3"],
        "stop_lat_origin": transfer_stop[2],
        "stop_lon_origin": transfer_stop[3],
        "stop_name_origin": transfer_stop[1],
        "stop_lat_dest": dest_stop[2],
        "stop_lon_dest": dest_stop[3]
    }

    leg1_geom = get_direct_trip_geometry(cur, leg1_trip_details, transport_map[best_option["trip1"]], search_shapes=True)
    leg2_geom = get_direct_trip_geometry(cur, leg2_trip_details, transport_map[best_option["trip2"]], search_shapes=True)

    conn.close()

    return {
        "status": "Found",
        "details": [{
            "origin": {"id": origin_stop[0], "name": origin_stop[1], "lat": origin_stop[2], "lon": origin_stop[3]},
            "transfer": {"id": transfer_stop[0], "name": transfer_stop[1], "lat": transfer_stop[2], "lon": transfer_stop[3]},
            "destination": {"id": dest_stop[0], "name": dest_stop[1], "lat": dest_stop[2], "lon": dest_stop[3]},
            "leg1": {"trip_details": leg1_trip_details, "transport_details": transport_map[best_option["trip1"]], "trip_geometry": leg1_geom},
            "leg2": {"trip_details": leg2_trip_details, "transport_details": transport_map[best_option["trip2"]], "trip_geometry": leg2_geom},
            "total_time": best_option["total_time"],
            "wait_time": best_option["wait_time"],
            "now_time": now_text
        }]
    }