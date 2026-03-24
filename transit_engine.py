import requests
import zipfile
import io
import gc
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
    del now_sf

    # max_time dinámico según distancia al destino
    straight_distance = haversine_distance(
        origin_coords[1], origin_coords[0],
        dest_coords[1], dest_coords[0]
    )
    estimated_travel_sec = (straight_distance / 30000) * 3600  # 30 km/h promedio
    max_time = current_sec + int(estimated_travel_sec) + 3600   # + 1h buffer
    max_time = min(max_time, current_sec + 10800)               # cap en 3h
    del straight_distance, estimated_travel_sec

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
    gc.collect()

    # --- 2. connections ---
    cur.execute("""
        SELECT from_stop, to_stop, departure_sec, arrival_sec, trip_id
        FROM connections
        WHERE departure_sec >= %s AND departure_sec <= %s
          AND arrival_sec IS NOT NULL
        ORDER BY departure_sec
    """, (current_sec, max_time))
    del max_time

    connections = cur.fetchall()

    # --- 3. CSA sin trasbordos (max_transfers = 0) ---
    earliest  = {}
    prev      = {}
    trip_used = {}

    for s in origin_ids:
        earliest[s]  = current_sec
        trip_used[s] = None
    del origin_ids

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

    del connections, dest_ids
    gc.collect()

    if not best_target:
        conn.close()
        return {"status": "Not found", "reason": "No direct trips were found between the origin and destination"}

    # --- 4. reconstruir path ---
    path = []
    cur_stop = best_target
    while cur_stop in prev:
        p = prev[cur_stop]
        path.append((p[0], cur_stop, p[1], p[2], p[3]))  # (from_stop, to_stop, trip_id, dep, arr)
        cur_stop = p[0]
    path.reverse()

    trip_id        = path[0][2]
    origin_dep     = path[0][3]
    origin_stop_id = path[0][0]
    dest_arr       = earliest[best_target]
    wait_time      = origin_dep - current_sec
    travel_time    = dest_arr - origin_dep
    total_time     = dest_arr - current_sec
    dest_stop_id   = best_target
    path_len       = len(path)
    del path, earliest, prev, trip_used
    gc.collect()

    # --- 5. stops ---
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops WHERE stop_id = ANY(%s)
    """, ([origin_stop_id, dest_stop_id],))
    stops_map   = {row[0]: row for row in cur.fetchall()}
    origin_stop = stops_map[origin_stop_id]
    dest_stop   = stops_map[dest_stop_id]
    del stops_map, origin_stop_id, dest_stop_id

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
    del row, trip_id

    # --- 7. geometry ---
    trip_details_for_geom = {
        "trip_id":              transport["trip_id"],
        "operator_id_origin":   transport["operator_id"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest":   path_len,
        "stop_name_origin":     origin_stop[1],
        "stop_lat_origin":      origin_stop[2],
        "stop_lon_origin":      origin_stop[3],
        "stop_lat_dest":        dest_stop[2],
        "stop_lon_dest":        dest_stop[3],
    }
    del path_len
    trip_geometry = get_direct_trip_geometry(cur, trip_details_for_geom, transport, search_shapes=True)
    del trip_details_for_geom

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
    del now_sf

    straight_distance = haversine_distance(
        origin_coords[1], origin_coords[0],
        dest_coords[1], dest_coords[0]
    )
    estimated_travel_sec = (straight_distance / 30000) * 3600
    max_time = current_sec + int(estimated_travel_sec) + 3600
    max_time = min(max_time, current_sec + 10800)
    del straight_distance, estimated_travel_sec

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
    gc.collect()

    # --- 2. connections ---
    cur.execute("""
        SELECT from_stop, to_stop, departure_sec, arrival_sec, trip_id, service_key
        FROM connections
        WHERE departure_sec >= %s AND departure_sec <= %s
          AND arrival_sec IS NOT NULL
        ORDER BY departure_sec
    """, (current_sec, max_time))

    connections = cur.fetchall()

    # --- 2b. footpaths ---
    stops_in_connections = set()
    for fc, tc, _, _, _, _ in connections:
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
    del stops_in_connections

    footpaths_from = {}
    for fp_from, fp_to, walk_sec in cur.fetchall():
        footpaths_from.setdefault(fp_from, []).append((fp_to, walk_sec))

    # --- 3. CSA con estado por (stop_id, transfers_used, last_service_key) ---
    earliest = {}
    prev = {}
    states_by_stop = {}

    for s in origin_ids:
        key = (s, 0, None)
        earliest[key] = current_sec
        prev[key] = None
        states_by_stop.setdefault(s, set()).add(key)
    del origin_ids

    best_target_key = None

    for from_stop, to_stop, dep, arr, trip, service_key in connections:

        from_states = states_by_stop.get(from_stop)
        if not from_states:
            continue

        for prev_key in tuple(from_states):
            best_arrival = earliest.get(prev_key)
            if best_arrival is None or dep < best_arrival:
                continue

            _, transfers_used, last_service_key = prev_key

            if last_service_key is None or last_service_key == service_key:
                new_transfers = transfers_used
            else:
                new_transfers = transfers_used + 1

            if new_transfers > 1:
                continue

            new_key = (to_stop, new_transfers, service_key)

            if new_key not in earliest or arr < earliest[new_key]:
                earliest[new_key] = arr
                prev[new_key] = (prev_key, from_stop, to_stop, trip, service_key, dep, arr)
                states_by_stop.setdefault(to_stop, set()).add(new_key)

                if to_stop in dest_ids:
                    if best_target_key is None or arr < earliest[best_target_key]:
                        best_target_key = new_key

                # propagar caminata sin sumar trasbordo
                for fp_to, walk_sec in footpaths_from.get(to_stop, []):
                    walk_arr = arr + walk_sec
                    if walk_arr > max_time:
                        continue

                    walk_key = (fp_to, new_transfers, None)

                    if walk_key not in earliest or walk_arr < earliest[walk_key]:
                        earliest[walk_key] = walk_arr
                        prev[walk_key] = (new_key, to_stop, fp_to, '__walk__', None, arr, walk_arr)
                        states_by_stop.setdefault(fp_to, set()).add(walk_key)

    del footpaths_from, connections, dest_ids, max_time, states_by_stop
    gc.collect()

    if not best_target_key:
        conn.close()
        return {"status": "Not found", "reason": "No trips with transfer in the next hour"}

    # --- 4. reconstruir path ---
    path = []
    cur_key = best_target_key

    while prev.get(cur_key) is not None:
        p = prev[cur_key]
        prev_key, from_stop, to_stop, trip_id, service_key, dep, arr = p
        path.append((from_stop, to_stop, trip_id, service_key, dep, arr))
        cur_key = prev_key

    path.reverse()
    del prev, earliest
    gc.collect()

    print("best_target_key")
    print(best_target_key)
    print("path")
    print(path)

    # separar bloques de transporte usando __walk__
    ride_blocks = []
    current_block = []

    for step in path:
        trip_id = step[2]

        if trip_id == '__walk__':
            if current_block:
                ride_blocks.append(current_block)
                current_block = []
        else:
            current_block.append(step)

    if current_block:
        ride_blocks.append(current_block)

    del path, current_block
    gc.collect()

    if len(ride_blocks) != 2:
        conn.close()
        return {"status": "Not found", "reason": "Route exceeds 1 transfer"}

    leg1_steps = ride_blocks[0]
    leg2_steps = ride_blocks[1]
    del ride_blocks

    leg1 = (leg1_steps[0][2], leg1_steps)
    leg2 = (leg2_steps[0][2], leg2_steps)

    print("leg1:", leg1)
    print("leg2:", leg2)

    # --- 5. tiempos reales ---
    first_departure = leg1_steps[0][4]
    first_arrival = leg1_steps[-1][5]
    second_departure = leg2_steps[0][4]
    final_arrival = leg2_steps[-1][5]

    wait_for_first_bus = first_departure - current_sec
    transfer_wait = second_departure - first_arrival
    leg1_duration = first_arrival - first_departure
    leg2_duration = final_arrival - second_departure
    total_time = final_arrival - current_sec

    print("first_departure -> ", first_departure)
    print("first_arrival -> ", first_arrival)
    print("second_departure -> ", second_departure)
    print("final_arrival -> ", final_arrival)

    # --- 6. stops ---
    trip1_origin_id = leg1[1][0][0]
    trip1_dest_id = leg1[1][-1][1]
    trip2_origin_id = leg2[1][0][0]
    trip2_dest_id = best_target_key[0]

    leg1_seq_dest = len(leg1[1])
    leg2_seq_dest = len(leg2[1])

    all_stop_ids = list({trip1_origin_id, trip1_dest_id, trip2_origin_id, trip2_dest_id})
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE stop_id = ANY(%s)
    """, (all_stop_ids,))
    del all_stop_ids

    stops_map = {row[0]: row for row in cur.fetchall()}

    trip1_origin_stop = stops_map[trip1_origin_id]
    trip1_dest_stop = stops_map[trip1_dest_id]
    trip2_origin_stop = stops_map[trip2_origin_id]
    trip2_dest_stop = stops_map[trip2_dest_id]
    del stops_map, trip1_origin_id, trip1_dest_id, trip2_origin_id, trip2_dest_id

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
    del trip_ids

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

    print(
        f"first_departure: {sec_to_time(first_departure)}, "
        f"first_arrival: {sec_to_time(first_arrival)}, "
        f"second_departure: {sec_to_time(second_departure)}, "
        f"final_arrival: {sec_to_time(final_arrival)}"
    )
    print(
        f"leg1_duration: {leg1_duration}, leg2_duration: {leg2_duration}, "
        f"wait_for_first_bus: {wait_for_first_bus}, transfer_wait: {transfer_wait}, total_time: {total_time}"
    )

    # --- 8. legs ---
    leg1_trip_details = {
        "trip_id": leg1[0],
        "operator_id_origin": transport_map[leg1[0]]["operator_id"],
        "route_type": transport_map[leg1[0]]["route_type"],
        "route_color": transport_map[leg1[0]]["route_color"],
        "route_short_name": transport_map[leg1[0]]["route_short_name"],
        "route_long_name": transport_map[leg1[0]]["route_long_name"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest": leg1_seq_dest,
        "stop_name_origin": trip1_origin_stop[1],
        "stop_lat_origin": trip1_origin_stop[2],
        "stop_lon_origin": trip1_origin_stop[3],
        "stop_lat_dest": trip1_dest_stop[2],
        "stop_lon_dest": trip1_dest_stop[3],
        "wait_for_first_bus": wait_for_first_bus,
        "departure_time_first_trip": sec_to_time(first_departure),
        "arrival_time_first_trip": sec_to_time(first_arrival),
        "travel_time": leg1_duration,
    }

    leg2_trip_details = {
        "trip_id": leg2[0],
        "operator_id_origin": transport_map[leg2[0]]["operator_id"],
        "route_type": transport_map[leg2[0]]["route_type"],
        "route_color": transport_map[leg2[0]]["route_color"],
        "route_short_name": transport_map[leg2[0]]["route_short_name"],
        "route_long_name": transport_map[leg2[0]]["route_long_name"],
        "stop_sequence_origin": 0,
        "stop_sequence_dest": leg2_seq_dest,
        "stop_name_origin": trip2_origin_stop[1],
        "stop_lat_origin": trip2_origin_stop[2],
        "stop_lon_origin": trip2_origin_stop[3],
        "stop_lat_dest": trip2_dest_stop[2],
        "stop_lon_dest": trip2_dest_stop[3],
        "transfer_wait": transfer_wait,
        "arrival_time_second_trip": sec_to_time(second_departure),
        "dest_arrival_time": sec_to_time(final_arrival),
        "travel_time": leg2_duration,
    }

    leg1_geom = get_direct_trip_geometry(cur, leg1_trip_details, transport_map[leg1[0]], search_shapes=True)
    leg2_geom = get_direct_trip_geometry(cur, leg2_trip_details, transport_map[leg2[0]], search_shapes=True)

    conn.close()

    return {
        "status": "Found",
        "details": [{
            "origin": {
                "id": trip1_origin_stop[0],
                "name": trip1_origin_stop[1],
                "lat": trip1_origin_stop[2],
                "lon": trip1_origin_stop[3]
            },
            "trip1_dest": {
                "id": trip1_dest_stop[0],
                "name": trip1_dest_stop[1],
                "lat": trip1_dest_stop[2],
                "lon": trip1_dest_stop[3]
            },
            "trip2_origin": {
                "id": trip2_origin_stop[0],
                "name": trip2_origin_stop[1],
                "lat": trip2_origin_stop[2],
                "lon": trip2_origin_stop[3]
            },
            "destination": {
                "id": trip2_dest_stop[0],
                "name": trip2_dest_stop[1],
                "lat": trip2_dest_stop[2],
                "lon": trip2_dest_stop[3]
            },
            "leg1": {
                "trip_details": leg1_trip_details,
                "transport_details": transport_map[leg1[0]],
                "trip_geometry": leg1_geom
            },
            "leg2": {
                "trip_details": leg2_trip_details,
                "transport_details": transport_map[leg2[0]],
                "trip_geometry": leg2_geom
            },
            "total_time": total_time,
            "wait_time": 0,
            "now_time": now_text
        }]
    }