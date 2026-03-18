import requests
import zipfile
import io
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime
import time

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

def time_to_seconds(t):
    """Convert GTFS HH:MM:SS to seconds"""
    if pd.isna(t):
        return None
    h, m, s = map(int, t.split(":"))
    return h*3600 + m*60 + s


def find_trip_with_transfer(origin_coords, dest_coords, search_radius=800, nearest_stops=100):
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
    WITH origin AS (
        SELECT stop_id
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
    ),
    dest AS (
        SELECT stop_id
        FROM stops
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            %s
        )
    ),
    first_leg AS (
        SELECT 
            trip_id,
            stop_id,
            arrival_sec,
            stop_sequence
        FROM stop_times
        WHERE 
            stop_id IN (SELECT stop_id FROM origin)
            AND arrival_sec IS NOT NULL
            AND arrival_sec >= %s                -- ahora (tiempo actual)
            AND arrival_sec <= %s + 3600         -- ventana de 1h
        ORDER BY arrival_sec
    ),
    transfers AS (
        SELECT 
            st1.trip_id AS trip1,
            st2.trip_id AS trip2,
            st1.stop_id AS transfer_stop,
            st1.arrival_sec AS t1,
            st2.departure_sec AS t2,
            st1.stop_sequence AS seq1,
            st2.stop_sequence AS seq2
        FROM first_leg st1
        JOIN stop_times st2
            ON st1.stop_id = st2.stop_id
        WHERE
            st2.departure_sec BETWEEN st1.arrival_sec + 60
                                AND st1.arrival_sec + 1800
    ),
    final_routes AS (
        SELECT 
            t.trip1,
            t.trip2,
            t.transfer_stop,
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
            AND st3.arrival_sec <= t.t1 + 7200   -- máximo 2h total
    )
    SELECT *,
        (dest_time - t1) AS total_travel_time
    FROM final_routes
    ORDER BY total_travel_time
    LIMIT 20;
    """
    now = time.localtime()
    current_sec = now.tm_hour*3600 + now.tm_min*60 + now.tm_sec
    params = (
        origin_coords[0], origin_coords[1], search_radius,  # origin ST_DWithin
        dest_coords[0], dest_coords[1], search_radius,    # dest ST_DWithin
        current_sec, current_sec                           # first_leg window
    )

    df = pd.read_sql(query, conn, params=params)
    if df.shape[0] > 0:
        # --- 1. Quedarse con las mejores 3 rutas ---
        df = df.sort_values("total_travel_time")
        df = df.drop_duplicates(subset=["transfer_stop", "dest_stop"], keep="first")
        df = df.head(3)
        routes = []
        cur = conn.cursor()
        # --- 1. Obtener origin stop UNA sola vez ---
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

        # --- 2. Obtener todos los stops necesarios en lote ---
        all_stop_ids = set(df["transfer_stop"]).union(set(df["dest_stop"]))

        cur.execute(f"""
            SELECT stop_id, stop_name, stop_lat, stop_lon
            FROM stops
            WHERE stop_id = ANY(%s);
        """, (list(all_stop_ids),))

        stops_map = {row[0]: row for row in cur.fetchall()}

        # --- 3. Obtener TODOS los stop_times en UNA query ---
        trip_ids = list(set(df["trip1"]).union(set(df["trip2"])))

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

        # --- 4. Agrupar por trip_id ---
        from collections import defaultdict

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

        # --- 5. Construir rutas ---
        for _, row in df.iterrows():

            trip1_stops = trip_map[row["trip1"]]
            trip2_stops = trip_map[row["trip2"]]

            # filtrar por secuencia
            leg1 = [s for s in trip1_stops if s["seq"] <= row["seq1"]]
            leg2 = [s for s in trip2_stops if s["seq"] >= row["seq2"]]

            transfer_stop = stops_map[row["transfer_stop"]]
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
        return {
            "status": "Found",
            "routes": routes
            #"best_route": best_route.to_dict(),
            #"alternatives": best_routes.to_dict("records"),
            #"routes_found": len(routes)
        }
    else:
        return {
            "status":"Not found"
        }


def run():
    result = find_trip_with_transfer((-122.4120372,37.7803603),(-122.4785598,37.8199109))
    print(result)