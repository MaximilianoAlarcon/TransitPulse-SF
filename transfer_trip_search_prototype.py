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
        SELECT *
        FROM stop_times
        WHERE stop_id IN (SELECT stop_id FROM origin) AND arrival_sec IS NOT NULL
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
        FROM stop_times st1
        JOIN stop_times st2
            ON st1.stop_id = st2.stop_id
        WHERE
            st1.trip_id IN (SELECT trip_id FROM first_leg)
            AND st2.departure_sec > st1.arrival_sec
            AND st2.departure_sec < st1.arrival_sec + 3600
    ),
    final_routes AS (
        SELECT 
            t.trip1,
            t.trip2,
            t.transfer_stop,
            t.t1,
            t.t2,
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

    params = (
        origin_coords[0],  # lon
        origin_coords[1],  # lat
        search_radius,               # radio origen (metros)
        dest_coords[0],    # lon
        dest_coords[1],    # lat
        search_radius                # radio destino
    )

    df = pd.read_sql(query, conn, params=params)
    if df.shape[0] > 0:
        # --- 1. Quedarse con las mejores 3 rutas ---
        df = df.sort_values("total_travel_time")
        df = df.drop_duplicates(subset=["transfer_stop", "dest_stop"], keep="first")
        df = df.head(3)
        routes = []
        cur = conn.cursor()
        for _, row in df.iterrows():
            # --- 2. Parada origen (la más cercana al usuario) ---
            cur.execute("""
                SELECT stop_id, stop_name, stop_lat, stop_lon
                FROM stops
                WHERE stop_id IN (
                    SELECT stop_id FROM stops
                    WHERE ST_DWithin(
                        geom::geography,
                        ST_SetSRID(ST_Point(%s,%s),4326)::geography,
                        %s
                    )
                )
                ORDER BY ST_Distance(
                    geom::geography,
                    ST_SetSRID(ST_Point(%s,%s),4326)::geography
                )
                LIMIT 1;
            """, (origin_coords[0], origin_coords[1], search_radius, origin_coords[0], origin_coords[1]))

            origin_stop = cur.fetchone()

            # --- 3. Transfer y destino ---
            cur.execute("""
                SELECT stop_id, stop_name, stop_lat, stop_lon
                FROM stops
                WHERE stop_id = %s;
            """, (row["transfer_stop"],))
            transfer_stop = cur.fetchone()

            cur.execute("""
                SELECT stop_id, stop_name, stop_lat, stop_lon
                FROM stops
                WHERE stop_id = %s;
            """, (row["dest_stop"],))
            dest_stop = cur.fetchone()

            # --- 4. Paradas tramo 1 ---
            cur.execute("""
                SELECT 
                    s.stop_id,
                    s.stop_name,
                    s.stop_lat,
                    s.stop_lon,
                    st.stop_sequence
                FROM stop_times st
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE st.trip_id = %s
                AND st.stop_sequence <= %s
                ORDER BY st.stop_sequence;
            """, (row["trip1"], row["seq1"]))

            leg1_stops = cur.fetchall()

            # --- 5. Paradas tramo 2 ---
            cur.execute("""
                SELECT 
                    s.stop_id,
                    s.stop_name,
                    s.stop_lat,
                    s.stop_lon,
                    st.stop_sequence
                FROM stop_times st
                JOIN stops s ON st.stop_id = s.stop_id
                WHERE st.trip_id = %s
                AND st.stop_sequence >= %s
                ORDER BY st.stop_sequence;
            """, (row["trip2"], row["seq2"]))

            leg2_stops = cur.fetchall()

            # --- 6. Armar estructura ---
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
                "leg1": [
                    {
                        "id": s[0],
                        "name": s[1],
                        "lat": s[2],
                        "lon": s[3],
                        "seq": s[4],
                    } for s in leg1_stops
                ],
                "leg2": [
                    {
                        "id": s[0],
                        "name": s[1],
                        "lat": s[2],
                        "lon": s[3],
                        "seq": s[4],
                    } for s in leg2_stops
                ],
                "total_time": row["total_travel_time"],
                "wait_time": row["t2"] - row["t1"]
            }
            routes.append(route)
        cur.close()
        # --- 7. Resultado final ---
        print(json.dumps(routes, indent=2))
        return {
            "status": "Found"
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