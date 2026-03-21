import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
from utils import get_direct_trip_geometry, time_to_seconds, estimate_radius

API_KEY = os.environ.get("API_511_KEY")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}


def find_trip_with_transfer(
    origin_coords,
    dest_coords,
    search_radius_origin=800,
    search_radius_dest=1200,
    transfer_radius=350,
    auto_estimate_radius=False
):
    """
    Busca rutas con un solo transbordo entre dos coordenadas.
    - origin_coords: (lon, lat)
    - dest_coords: (lon, lat)
    - search_radius_origin: radio para stops de origen (en metros)
    - search_radius_dest: radio para stops de destino (en metros)
    - transfer_radius: radio para considerar transbordo cercano (en metros)
    """

    conn = psycopg2.connect(**DB_CONFIG)

    if auto_estimate_radius:
        search_radius_origin = estimate_radius(conn, origin_coords)
        search_radius_dest = estimate_radius(conn, dest_coords)

    # hora actual en SF
    now_sf = datetime.now(ZoneInfo("America/Los_Angeles"))
    current_sec = now_sf.hour * 3600 + now_sf.minute * 60 + now_sf.second

    # Query con 14 placeholders %s
    query = """
    WITH origin AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    ),
    dest AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
            %s
        )
    ),
    first_leg AS (
        SELECT st.*
        FROM stop_times st
        JOIN stops s ON st.stop_id = s.stop_id
        WHERE
            st.stop_id IN (SELECT stop_id FROM origin)
            AND st.arrival_sec IS NOT NULL
            AND st.departure_sec >= %s
            AND st.departure_sec <= %s + 3600
            AND ST_Distance(
                s.geom::geography,
                ST_SetSRID(ST_Point(%s, %s), 4326)::geography
            )
            <
            ST_Distance(
                ST_SetSRID(ST_Point(%s, %s), 4326)::geography,
                ST_SetSRID(ST_Point(%s, %s), 4326)::geography
            ) * 1.2
        ORDER BY st.departure_sec
        LIMIT 150
    ),
    transfers AS (
        SELECT
            st1.trip_id   AS trip1,
            st2.trip_id   AS trip2,
            st1.stop_id   AS leg1_stop,
            st2.stop_id   AS leg2_stop,
            st1.arrival_sec   AS t1,
            st2.departure_sec AS t2,
            st1.stop_sequence AS seq1,
            st2.stop_sequence AS seq2,
            st2.arrival_time  AS arrival_time_second_trip
        FROM stop_times st1
        JOIN stops s1 ON st1.stop_id = s1.stop_id
        JOIN stop_times st2 ON st2.trip_id IS NOT NULL
        JOIN stops s2 ON st2.stop_id = s2.stop_id
        WHERE
            ST_DWithin(s1.geom::geography, s2.geom::geography, 200)
            AND st1.trip_id IN (SELECT trip_id FROM first_leg)
            AND st2.departure_sec > st1.arrival_sec
            AND st2.departure_sec >= %s
            AND st2.departure_sec < st1.arrival_sec + 7200
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
            st3.arrival_sec   AS dest_time,
            st3.stop_id       AS dest_stop,
            st3.stop_sequence AS seq3
        FROM transfers t
        JOIN stop_times st3 ON t.trip2 = st3.trip_id
        WHERE
            st3.stop_id IN (SELECT stop_id FROM dest)
            AND st3.stop_sequence > t.seq2
    )
    SELECT *,
        (dest_time - t1)      AS total_travel_time,
        (t1 - %s) AS wait_for_first_bus
    FROM final_routes
    ORDER BY total_travel_time
    LIMIT 20;
    """

    # Params en orden exacto de los %s
    params = (
        origin_coords[0], origin_coords[1], search_radius_origin,  # 3
        dest_coords[0], dest_coords[1], search_radius_dest,        # 3 → total 6
        current_sec, current_sec,                                   # 2 → total 8
        dest_coords[0],   dest_coords[1],                           # 2  → dist(stop → dest)
        origin_coords[0], origin_coords[1],                         # 2  → dist(origin → dest) punto A
        dest_coords[0],   dest_coords[1], 
        current_sec,                                                # st2.departure_sec >= %s → 13
        current_sec,
    )

    # Ejecutar query y traer df
    df = pd.read_sql(query, conn, params=params)

    if df.shape[0] == 0:
        return {
            "status": "Not found",
            "reason": "We found no trips with transfers within the next hour"
        }

    # Mantener solo rutas válidas (tiempo total positivo)
    df = df[df["total_travel_time"] >= 0]
    # FIX Bug 1 (complemento): descartar filas donde el bus ya pasó
    df = df[df["wait_for_first_bus"] >= 0]
    df = df.sort_values("total_travel_time")
    df = df.drop_duplicates(subset=["leg2_stop", "dest_stop"], keep="first")
    df = df.head(1)

    if df.shape[0] == 0:
        return {
            "status": "Not found",
            "reason": "All found trips have already departed"
        }

    routes = []
    cur = conn.cursor()

    # Obtener stop más cercano al origen
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        ORDER BY ST_Distance(
            geom::geography,
            ST_SetSRID(ST_Point(%s, %s), 4326)::geography
        )
        LIMIT 1;
    """, (origin_coords[0], origin_coords[1]))
    origin_stop = cur.fetchone()

    # Obtener stops necesarios en lote
    all_stop_ids = (
        set(df["leg1_stop"])
        .union(df["leg2_stop"])
        .union(df["dest_stop"])
    )
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE stop_id = ANY(%s);
    """, (list(all_stop_ids),))
    stops_map = {row[0]: row for row in cur.fetchall()}

    # Obtener info de trips y rutas
    trip_ids = list(set(df["trip1"]).union(df["trip2"]))
    cur.execute("""
        SELECT
            t.trip_id,
            t.operator_id,
            r.route_id,
            r.route_type,
            r.route_color,
            r.route_short_name,
            r.route_long_name
        FROM trips t
        JOIN routes r
            ON t.route_id = r.route_id
            AND t.operator_id = r.operator_id
        WHERE t.trip_id = ANY(%s)
    """, (trip_ids,))

    transport_map = {
        row[0]: {
            "operator_id":    row[1],
            "route_id":       row[2],
            "route_type":     row[3],
            "route_color":    row[4],
            "route_short_name": row[5],
            "route_long_name":  row[6]
        }
        for row in cur.fetchall()
    }

    for _, row in df.iterrows():

        trip1 = row["trip1"]
        trip2 = row["trip2"]

        transfer_stop = stops_map[row["leg2_stop"]]
        dest_stop     = stops_map[row["dest_stop"]]

        # ---------------------------
        # FIX Bug 3: obtener la secuencia real del stop de origen en trip1
        # ---------------------------
        cur.execute("""
            SELECT stop_sequence
            FROM stop_times
            WHERE trip_id = %s AND stop_id = %s
            LIMIT 1;
        """, (trip1, origin_stop[0]))
        origin_seq_row = cur.fetchone()
        origin_seq = origin_seq_row[0] if origin_seq_row else 0

        # ---------------------------
        # LEG 1
        # ---------------------------
        leg1_trip_details = {
            "trip_id":             trip1,
            "operator_id_origin":  transport_map[trip1]["operator_id"],
            "route_type":          transport_map[trip1]["route_type"],
            "route_long_name":     transport_map[trip1]["route_long_name"],
            "route_short_name":    transport_map[trip1]["route_short_name"],
            "route_color":         transport_map[trip1]["route_color"],
            "stop_name_origin":    origin_stop[1],
            "stop_lat_origin":     origin_stop[2],
            "stop_lon_origin":     origin_stop[3],
            "stop_lat_dest":       transfer_stop[2],
            "stop_lon_dest":       transfer_stop[3],
            # FIX Bug 3: secuencia real del stop de origen
            "stop_sequence_origin": origin_seq,
            "stop_sequence_dest":   row["seq1"],
            # FIX Bug 1: tiempo en segundos hasta que llega el primer bus (siempre >= 0)
            "wait_for_first_bus":   row["wait_for_first_bus"],
        }

        leg1_transport = transport_map[trip1]

        # ---------------------------
        # LEG 2
        # ---------------------------
        leg2_trip_details = {
            "trip_id":             trip2,
            "operator_id_origin":  transport_map[trip2]["operator_id"],
            "route_type":          transport_map[trip2]["route_type"],
            "route_long_name":     transport_map[trip2]["route_long_name"],
            "route_short_name":    transport_map[trip2]["route_short_name"],
            # FIX Bug 2: era transport_map[trip1], ahora usa trip2 correctamente
            "route_color":         transport_map[trip2]["route_color"],
            "stop_name_origin":    transfer_stop[1],
            "stop_lat_origin":     transfer_stop[2],
            "stop_lon_origin":     transfer_stop[3],
            "stop_lat_dest":       dest_stop[2],
            "stop_lon_dest":       dest_stop[3],
            "stop_sequence_origin": row["seq2"],
            "stop_sequence_dest":   row["seq3"],
            "arrival_time_second_trip": row["arrival_time_second_trip"],
        }

        leg2_transport = transport_map[trip2]

        # ---------------------------
        # ROUTE FINAL
        # ---------------------------
        leg1_trip_geometry = get_direct_trip_geometry(cur, leg1_trip_details, leg1_transport)
        leg2_trip_geometry = get_direct_trip_geometry(cur, leg2_trip_details, leg2_transport)

        route = {
            "origin": {
                "id":   origin_stop[0],
                "name": origin_stop[1],
                "lat":  origin_stop[2],
                "lon":  origin_stop[3],
            },
            "transfer": {
                "id":   transfer_stop[0],
                "name": transfer_stop[1],
                "lat":  transfer_stop[2],
                "lon":  transfer_stop[3],
            },
            "destination": {
                "id":   dest_stop[0],
                "name": dest_stop[1],
                "lat":  dest_stop[2],
                "lon":  dest_stop[3],
            },
            "leg1": {
                "trip_details":    leg1_trip_details,
                "transport_details": leg1_transport,
                "trip_geometry":   leg1_trip_geometry,
            },
            "leg2": {
                "trip_details":    leg2_trip_details,
                "transport_details": leg2_transport,
                "trip_geometry":   leg2_trip_geometry,
            },
            "total_time": row["total_travel_time"],
            "wait_time":  row["t2"] - row["t1"],
        }

        routes.append(route)

    cur.close()
    conn.close()
    return {"status": "Found", "details": routes}