import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from utils import get_direct_trip_geometry,estimate_radius,should_use_transit,time_to_seconds,estimate_radius_and_limit,haversine_distance
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

    if not should_use_transit(origin_coords, dest_coords):
        return {"status": "Canceled", "reason": "You should go walking"}

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    if auto_estimate_radius:
        search_radius_origin = estimate_radius(conn, origin_coords)
        search_radius_dest   = estimate_radius(conn, dest_coords)

    now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
    now_text = now_sf.strftime("%H:%M")
    current_sec = now_sf.hour * 3600 + now_sf.minute * 60 + now_sf.second

    # max_time dinámico según distancia al destino
    straight_distance = haversine_distance(
        origin_coords[1], origin_coords[0],
        dest_coords[1], dest_coords[0]
    )
    estimated_travel_sec = (straight_distance / 30000) * 3600  # 30 km/h promedio
    max_time = current_sec + int(estimated_travel_sec) + 3600   # + 1h buffer
    max_time = min(max_time, current_sec + 10800)               # cap en 3h

    # --- 1. stops cercanos ---
    origin_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    """, conn, params=(origin_coords[0], origin_coords[1], search_radius_origin))

    dest_stops = pd.read_sql("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    """, conn, params=(dest_coords[0], dest_coords[1], search_radius_dest))

    if origin_stops.empty:
        conn.close()
        return {"status": "Not found", "reason": "There is no stop near the origin"}

    if dest_stops.empty:
        conn.close()
        return {"status": "Not found", "reason": "There is no stop near the destination"}

    origin_ids = set(origin_stops["stop_id"])
    dest_ids   = set(dest_stops["stop_id"])
    del origin_stops, dest_stops

    # --- 2. connections ---
    cur.execute("""
        SELECT from_stop, to_stop, departure_sec, arrival_sec, trip_id
        FROM connections
        WHERE departure_sec >= %s AND departure_sec <= %s
          AND arrival_sec IS NOT NULL
        ORDER BY departure_sec
    """, (current_sec, max_time))

    connections = cur.fetchall()

    # --- 3. CSA sin trasbordos (max_transfers = 0) ---
    earliest  = {}
    prev      = {}
    trip_used = {}

    for s in origin_ids:
        earliest[s]  = current_sec
        trip_used[s] = None

    best_target = None

    for from_stop, to_stop, dep, arr, trip in connections:

        if from_stop not in earliest:
            continue
        if dep < earliest[from_stop]:
            continue

        # sin trasbordos: si ya se usó un trip distinto en from_stop, saltar
        prev_trip = trip_used[from_stop]
        if prev_trip is not None and prev_trip != trip:
            continue

        if to_stop not in earliest or arr < earliest[to_stop]:
            earliest[to_stop] = arr
            trip_used[to_stop] = trip
            prev[to_stop] = (from_stop, trip, dep, arr)

            if to_stop in dest_ids:
                best_target = to_stop
                break

    if not best_target:
        conn.close()
        return {"status": "Not found", "reason": "No direct trips were found between the origin and destination"}

    del connections, dest_ids

    # --- 4. reconstruir path ---
    path = []
    cur_stop = best_target
    while cur_stop in prev:
        p = prev[cur_stop]
        path.append((p[0], cur_stop, p[1], p[2], p[3]))  # (from_stop, to_stop, trip_id, dep, arr)
        cur_stop = p[0]
    path.reverse()

    trip_id      = path[0][2]
    origin_dep   = path[0][3]
    dest_arr     = earliest[best_target]
    wait_time    = origin_dep - current_sec
    travel_time  = dest_arr - origin_dep
    total_time   = dest_arr - current_sec

    # --- 5. stops ---
    origin_stop_id = path[0][0]
    dest_stop_id   = best_target
    del earliest, prev, trip_used, path

    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops WHERE stop_id = ANY(%s)
    """, ([origin_stop_id, dest_stop_id],))
    stops_map   = {row[0]: row for row in cur.fetchall()}
    origin_stop = stops_map[origin_stop_id]
    dest_stop   = stops_map[dest_stop_id]
    del stops_map

    # --- 6. transport ---
    cur.execute("""
        SELECT t.trip_id, t.operator_id, t.route_id,
               r.route_type, r.route_color, r.route_short_name,
               r.route_long_name, r.route_desc, r.route_url
        FROM trips t
        JOIN routes r ON t.route_id = r.route_id AND t.operator_id = r.operator_id
        WHERE t.trip_id = %s
        LIMIT 1
    """, (trip_id,))
    row = cur.fetchone()
    transport = {
        "trip_id":          row[0],
        "operator_id":      row[1],
        "route_id":         row[2],
        "route_type":       row[3],
        "route_color":      row[4],
        "route_short_name": row[5],
        "route_long_name":  row[6],
        "route_desc":       row[7],
        "route_url":        row[8],
    }

    # --- 7. geometry ---
    trip_details_for_geom = {
        "trip_id":              trip_id,
        "operator_id_origin":   transport["operator_id"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest":   len(path),
        "stop_name_origin":     origin_stop[1],
        "stop_lat_origin":      origin_stop[2],
        "stop_lon_origin":      origin_stop[3],
        "stop_lat_dest":        dest_stop[2],
        "stop_lon_dest":        dest_stop[3],
    }
    trip_geometry = get_direct_trip_geometry(cur, trip_details_for_geom, transport, search_shapes=True)

    conn.close()

    return {
        "status": "Found",
        "details": {
            "stop_name_origin":  origin_stop[1],
            "arrival_time_origin": f"{origin_dep // 3600 % 24:02d}:{(origin_dep % 3600) // 60:02d}",
            "stop_name_dest":    dest_stop[1],
            "arrival_time_dest": f"{dest_arr // 3600 % 24:02d}:{(dest_arr % 3600) // 60:02d}",
            "stop_lat_origin":   float(origin_stop[2]),
            "stop_lon_origin":   float(origin_stop[3]),
            "stop_lat_dest":     float(dest_stop[2]),
            "stop_lon_dest":     float(dest_stop[3]),
            "travel_time":       travel_time,
            "wait_time":         wait_time,
            "total_time":        total_time,
            "route_short_name":  transport["route_short_name"],
            "route_long_name":   transport["route_long_name"],
            "route_desc":        transport["route_desc"],
            "route_type":        transport["route_type"],
            "route_url":         transport["route_url"],
            "route_color":       transport["route_color"],
            "trip_geometry":     trip_geometry
        }
    }

def sec_to_time(sec):
    h = int(sec // 3600) % 24
    m = int((sec % 3600) // 60)
    return f"{h:02d}:{m:02d}"

MAX_WAIT_FOR_FIRST_BUS = 3600
MAX_WAIT_FOR_SECOND_BUS = 3600
 
def find_trip_with_transfer(origin_coords, dest_coords, search_radius_origin=800, search_radius_dest=1200, transfer_radius=350, auto_estimate_radius=False):

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
    now_text = now_sf.strftime("%H:%M")
    current_sec = now_sf.hour * 3600 + now_sf.minute * 60 + now_sf.second

    # max_time dinámico según distancia al destino
    # 30 km/h promedio en transporte + 1h de buffer para esperas y trasbordos
    straight_distance = haversine_distance(
        origin_coords[1], origin_coords[0],
        dest_coords[1], dest_coords[0]
    )
    estimated_travel_sec = (straight_distance / 30000) * 3600  # 30 km/h
    max_time = current_sec + int(estimated_travel_sec) + 3600   # + 1h buffer
    max_time = min(max_time, current_sec + 10800)               # cap en 3h

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
    del origin_stops, dest_stops

    # --- 2. connections ---
    cur.execute("""
        SELECT from_stop, to_stop, departure_sec, arrival_sec, trip_id
        FROM connections
        WHERE departure_sec >= %s AND departure_sec <= %s 
        AND arrival_sec IS NOT NULL 
        ORDER BY departure_sec
    """, (current_sec, max_time))

    connections = cur.fetchall()

    # --- 2b. footpaths: paradas cercanas para modelar caminata en el trasbordo ---
    # Se calculan dinámicamente entre todas las paradas que aparecen en connections
    stops_in_connections = set()
    for fc, tc, _, _, _ in connections:
        stops_in_connections.add(fc)
        stops_in_connections.add(tc)

    cur.execute("""
        SELECT a.stop_id, b.stop_id,
               CEIL(ST_Distance(a.geom::geography, b.geom::geography) / 1.2)::int AS walk_sec
        FROM stops a
        JOIN stops b ON a.stop_id != b.stop_id
        WHERE a.stop_id = ANY(%s)
          AND b.stop_id = ANY(%s)
          AND ST_DWithin(a.geom::geography, b.geom::geography, %s)
    """, (list(stops_in_connections), list(stops_in_connections), transfer_radius))

    # índice de footpaths por parada origen para lookup rápido
    footpaths_from = {}
    for fp_from, fp_to, walk_sec in cur.fetchall():
        footpaths_from.setdefault(fp_from, []).append((fp_to, walk_sec))
    del stops_in_connections

    # --- 3. CSA con footpaths ---
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
            prev[to_stop] = (from_stop, trip, dep, arr)

            #print(f"dest_ids count: {len(dest_ids)}")

            if to_stop in dest_ids:
                best_target = to_stop

                #print(f"best_target encontrado: {to_stop}, arr: {arr} seg = {arr//3600:02d}:{(arr%3600)//60:02d}")
                
                break

            # propagar caminata desde to_stop hacia paradas cercanas
            for fp_to, walk_sec in footpaths_from.get(to_stop, []):
                walk_arr = arr + walk_sec
                if walk_arr > max_time:
                    continue
                if fp_to not in earliest or walk_arr < earliest[fp_to]:
                    earliest[fp_to] = walk_arr
                    trip_used[fp_to] = None   # reset: desde acá puede tomar cualquier trip
                    prev[fp_to] = (to_stop, '__walk__', arr, walk_arr)
        #print(f"best_target al salir del loop: {best_target}")
        #print(f"earliest keys count: {len(earliest)}")

    if not best_target:
        conn.close()
        return {"status": "Not found", "reason": "No trips with transfer in the next hour"}

    del connections, footpaths_from, dest_ids

    # --- 4. reconstruir path ---
    path = []
    cur_stop = best_target

    while cur_stop in prev:
        p = prev[cur_stop]
        path.append((p[0], cur_stop, p[1], p[2], p[3]))  # (from_stop, to_stop, trip_id, dep, arr)
        cur_stop = p[0]

    path.reverse()

    # separar legs por trip_id, ignorando pasos de caminata (__walk__)
    # un __walk__ separa los dos legs de colectivo pero no es un leg en sí
    legs = []
    current_leg = []
    current_trip = None

    for step in path:
        trip_id = step[2]
        if trip_id == '__walk__':
            if current_leg:
                legs.append((current_trip, current_leg))
                current_leg = []
                current_trip = None
        else:
            if current_trip is None:
                current_trip = trip_id
            if trip_id == current_trip:
                current_leg.append(step)
            else:
                legs.append((current_trip, current_leg))
                current_leg = [step]
                current_trip = trip_id

    if current_leg:
        legs.append((current_trip, current_leg))

    if len(legs) < 2:
        conn.close()
        return {"status": "Not found", "reason": "No transfer route found"}

    leg1, leg2 = legs[0], legs[1]
    del prev, earliest, trip_used, path, legs

    # --- 5. tiempos reales ---
    first_leg_steps = leg1[1]
    second_leg_steps = leg2[1]

    first_departure = first_leg_steps[0][3]
    second_departure = second_leg_steps[0][3]
    final_arrival = second_leg_steps[-1][4]
    first_arrival = first_leg_steps[-1][4]
    del first_leg_steps, second_leg_steps

    wait_for_first_bus = first_departure - current_sec

    # --- 6. stops: los 4 puntos del viaje con trasbordo ---
    # Trip 1 Origin → donde sube al primer colectivo  (from_stop del primer step del leg1)
    # Trip 1 Dest   → donde baja del primer colectivo (to_stop  del último step del leg1)
    # Trip 2 Origin → donde sube al segundo colectivo (from_stop del primer step del leg2)
    # Trip 2 Dest   → best_target (parada dentro del radio destino)
    # Con footpaths, trip1_dest != trip2_origin cuando hay caminata entre paradas
    trip1_origin_id = leg1[1][0][0]
    trip1_dest_id   = leg1[1][-1][1]
    trip2_origin_id = leg2[1][0][0]
    trip2_dest_id   = best_target

    all_stop_ids = list({trip1_origin_id, trip1_dest_id, trip2_origin_id, trip2_dest_id})
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE stop_id = ANY(%s)
    """, (all_stop_ids,))

    stops_map = {row[0]: row for row in cur.fetchall()}

    trip1_origin_stop = stops_map[trip1_origin_id]
    trip1_dest_stop   = stops_map[trip1_dest_id]
    trip2_origin_stop = stops_map[trip2_origin_id]
    trip2_dest_stop   = stops_map[trip2_dest_id]
    del stops_map

    # --- 7. transport ---
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

    # --- 8. legs ---
    # leg1: Trip 1 Origin → Trip 1 Dest
    leg1_trip_details = {
        "trip_id": leg1[0],
        "operator_id_origin": transport_map[leg1[0]]["operator_id"],
        "route_type": transport_map[leg1[0]]["route_type"],
        "route_color": transport_map[leg1[0]]["route_color"],
        "route_short_name": transport_map[leg1[0]]["route_short_name"],
        "route_long_name": transport_map[leg1[0]]["route_long_name"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest": len(leg1[1]),
        "stop_name_origin": trip1_origin_stop[1],
        "stop_lat_origin": trip1_origin_stop[2],
        "stop_lon_origin": trip1_origin_stop[3],
        "stop_lat_dest": trip1_dest_stop[2],
        "stop_lon_dest": trip1_dest_stop[3],
    }

    # leg2: Trip 2 Origin → Trip 2 Dest
    # Trip 2 Origin puede ser distinto a Trip 1 Dest (el pasajero camina entre paradas)
    leg2_trip_details = {
        "trip_id": leg2[0],
        "operator_id_origin": transport_map[leg2[0]]["operator_id"],
        "route_type": transport_map[leg2[0]]["route_type"],
        "route_color": transport_map[leg2[0]]["route_color"],
        "route_short_name": transport_map[leg2[0]]["route_short_name"],
        "route_long_name": transport_map[leg2[0]]["route_long_name"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest": len(leg2[1]),
        "stop_name_origin": trip2_origin_stop[1],
        "stop_lat_origin": trip2_origin_stop[2],
        "stop_lon_origin": trip2_origin_stop[3],
        "stop_lat_dest": trip2_dest_stop[2],
        "stop_lon_dest": trip2_dest_stop[3],
    }

    leg1_geom = get_direct_trip_geometry(cur, leg1_trip_details, transport_map[leg1[0]], search_shapes=True)
    leg2_geom = get_direct_trip_geometry(cur, leg2_trip_details, transport_map[leg2[0]], search_shapes=True)

    transfer_wait   = second_departure - first_arrival  # espera entre colectivos

    leg1_duration   = first_arrival - first_departure   # duración viaje 1
    leg2_duration   = final_arrival - second_departure  # duración viaje 2
    total_time      = wait_for_first_bus + leg1_duration + transfer_wait + leg2_duration

    print(f"first_departure: {sec_to_time(first_departure)}, first_arrival: {sec_to_time(first_arrival)}, second_departure: {sec_to_time(second_departure)}, final_arrival: {sec_to_time(final_arrival)}")
    print(f"leg1_duration: {leg1_duration}, leg2_duration: {leg2_duration}, wait_for_first_bus: {wait_for_first_bus}, transfer_wait: {transfer_wait}, total_time: {total_time}")

    leg1_trip_details["wait_for_first_bus"]        = wait_for_first_bus
    leg1_trip_details["departure_time_first_trip"] = sec_to_time(first_departure)
    leg1_trip_details["arrival_time_first_trip"]   = sec_to_time(first_arrival)
    leg1_trip_details["travel_time"]               = leg1_duration

    leg2_trip_details["transfer_wait"]             = transfer_wait
    leg2_trip_details["arrival_time_second_trip"]  = sec_to_time(second_departure)
    leg2_trip_details["dest_arrival_time"]         = sec_to_time(final_arrival)
    leg2_trip_details["travel_time"]               = leg2_duration

    conn.close()

    return {
        "status": "Found",
        "details": [{
            "origin":       {"id": trip1_origin_stop[0], "name": trip1_origin_stop[1], "lat": trip1_origin_stop[2], "lon": trip1_origin_stop[3]},
            "trip1_dest":   {"id": trip1_dest_stop[0],   "name": trip1_dest_stop[1],   "lat": trip1_dest_stop[2],   "lon": trip1_dest_stop[3]},
            "trip2_origin": {"id": trip2_origin_stop[0], "name": trip2_origin_stop[1], "lat": trip2_origin_stop[2], "lon": trip2_origin_stop[3]},
            "destination":  {"id": trip2_dest_stop[0],   "name": trip2_dest_stop[1],   "lat": trip2_dest_stop[2],   "lon": trip2_dest_stop[3]},
            "leg1": {"trip_details": leg1_trip_details, "transport_details": transport_map[leg1[0]], "trip_geometry": leg1_geom},
            "leg2": {"trip_details": leg2_trip_details, "transport_details": transport_map[leg2[0]], "trip_geometry": leg2_geom},
            "total_time": total_time,
            "wait_time": 0,
            "now_time": now_text
        }]
    }