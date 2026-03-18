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

API_KEY = os.environ.get("API_511_KEY")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST"),
    "database": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "port": os.environ.get("DB_PORT")
}

def time_to_seconds(t):
    """Convert GTFS HH:MM:SS to seconds"""
    if pd.isna(t):
        return None
    h, m, s = map(int, t.split(":"))
    return h*3600 + m*60 + s

def estimate_radius(conn, coords):
    lon, lat = coords

    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            500
        );
    """, (lon, lat))

    count = cur.fetchone()[0]
    cur.close()

    if count > 30:
        return 600
    elif count > 10:
        return 1000
    else:
        return 1500

def find_trip_with_transfer(origin_coords, dest_coords, search_radius_origin=800, search_radius_dest=1200, transfer_radius=350, auto_estimate_radius=False):
    """
    Busca rutas con un solo transbordo entre dos coordenadas.
    - origin_coords: (lon, lat)
    - dest_coords: (lon, lat)
    - search_radius: radio para stops de origen/destino (en metros)
    - transfer_radius: radio para considerar transbordo cercano (en metros)
    """

    conn = psycopg2.connect(**DB_CONFIG)

    if auto_estimate_radius:
        search_radius_origin =  estimate_radius(conn,origin_coords)
        search_radius_dest =  estimate_radius(conn,dest_coords)

    print("Radio para origen: "+str(search_radius_origin))
    print("Radio para destino: "+str(search_radius_dest))

    query = f"""
    WITH origin AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
    ),
    dest AS (
        SELECT stop_id, geom
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
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
            AND st.arrival_sec >= %s 
            AND st.arrival_sec <= %s + 3600

            -- 🔥 filtro direccional (con tolerancia)
            AND ST_Distance(
                s.geom::geography,
                ST_SetSRID(ST_Point(%s,%s),4326)::geography
            ) 
            < 
            ST_Distance(
                ST_SetSRID(ST_Point(%s,%s),4326)::geography,
                ST_SetSRID(ST_Point(%s,%s),4326)::geography
            ) * 1.2

        ORDER BY st.arrival_sec
        LIMIT 150
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
            st2.stop_sequence AS seq2
        FROM stop_times st1
        JOIN stops s1 ON st1.stop_id = s1.stop_id
        JOIN stop_times st2 ON st2.trip_id IS NOT NULL  -- mantiene todos los trips
        JOIN stops s2 ON st2.stop_id = s2.stop_id
        WHERE 
            ST_DWithin(s1.geom::geography, s2.geom::geography, 200)
            AND st1.trip_id IN (SELECT trip_id FROM first_leg)
            AND st2.departure_sec > st1.arrival_sec
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
            st3.arrival_sec AS dest_time,
            st3.stop_id AS dest_stop
        FROM transfers t
        JOIN stop_times st3
            ON t.trip2 = st3.trip_id
        WHERE
            st3.stop_id IN (SELECT stop_id FROM dest)
            AND st3.stop_sequence > t.seq2
    )
    SELECT *,
        (dest_time - t1) AS total_travel_time
    FROM final_routes
    ORDER BY total_travel_time
    LIMIT 20;
    """

    now_sf = datetime.now(ZoneInfo("America/Los_Angeles")) 
    current_sec = now_sf.hour*3600 + now_sf.minute*60 + now_sf.second

    params = (
        origin_coords[0], origin_coords[1], search_radius_origin,
        dest_coords[0], dest_coords[1], search_radius_dest,
        current_sec, current_sec,
        dest_coords[0], dest_coords[1],
        origin_coords[0], origin_coords[1],
        dest_coords[0], dest_coords[1]
    )

    df = pd.read_sql(query, conn, params=params)
    print("Dataframe:")
    print(df.head())
    
    if df.shape[0] == 0:
        return {"status": "Not found","reason":"We found no trips with transfers within the next hour"}

    # Mantener solo mejores 3 rutas sin duplicados
    df = df.sort_values("total_travel_time")
    df = df.drop_duplicates(subset=["leg2_stop", "dest_stop"], keep="first")
    df = df.head(3)

    routes = []
    cur = conn.cursor()

    # Obtener origen
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        ORDER BY ST_Distance(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography
        )
        LIMIT 1;
    """, (origin_coords[0], origin_coords[1]))
    origin_stop = cur.fetchone()

    # Obtener stops necesarios en lote
    all_stop_ids = set(df["leg1_stop"]).union(df["leg2_stop"]).union(df["dest_stop"])
    cur.execute("""
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        WHERE stop_id = ANY(%s);
    """, (list(all_stop_ids),))
    stops_map = {row[0]: row for row in cur.fetchall()}

    # Obtener todos los stop_times necesarios
    trip_ids = list(set(df["trip1"]).union(df["trip2"]))
    cur.execute("""
        SELECT 
            st.trip_id,
            st.stop_id,
            st.stop_sequence,
            s.stop_name,
            s.stop_lat,
            s.stop_lon
        FROM stop_times st
        JOIN stops s ON st.stop_id = s.stop_id
        WHERE st.trip_id = ANY(%s)
        ORDER BY st.trip_id, st.stop_sequence;
    """, (trip_ids,))
    all_stop_times = cur.fetchall()

    # Agrupar por trip_id
    trip_map = defaultdict(list)
    for row_st in all_stop_times:
        trip_id, stop_id, seq, name, lat, lon = row_st
        trip_map[trip_id].append({
            "id": stop_id,
            "name": name,
            "lat": lat,
            "lon": lon,
            "seq": seq
        })

    # Construir rutas
    for _, row in df.iterrows():
        trip1_stops = trip_map[row["trip1"]]
        trip2_stops = trip_map[row["trip2"]]

        leg1 = [s for s in trip1_stops if s["seq"] <= row["seq1"]]
        leg2 = [s for s in trip2_stops if s["seq"] >= row["seq2"]]

        transfer_stop = stops_map[row["leg2_stop"]]
        dest_stop = stops_map[row["dest_stop"]]

        route = {
            "origin": {
                "id": origin_stop[0],
                "name": origin_stop[1],
                "lat": origin_stop[2],
                "lon": origin_stop[3],
            },
            "transfer": {
                "id": transfer_stop[0],
                "name": transfer_stop[1],
                "lat": transfer_stop[2],
                "lon": transfer_stop[3],
            },
            "destination": {
                "id": dest_stop[0],
                "name": dest_stop[1],
                "lat": dest_stop[2],
                "lon": dest_stop[3],
            },
            "leg1": leg1,
            "leg2": leg2,
            "total_time": row["total_travel_time"],
            "wait_time": row["t2"] - row["t1"]
        }
        routes.append(route)

    cur.close()
    print(json.dumps(routes, indent=2))
    return {"status": "Found", "details": routes}